import asyncio
import logging
import socket
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("modbus_broker")

# Locks globais para garantir acesso exclusivo por socket/stream
modbus_lock = asyncio.Lock()
usr_reader_lock = asyncio.Lock()

last_notebook_activity = 0
NOTEBOOK_PRIORITY_TIMEOUT = 5.0

usr_reader = None
usr_writer = None

def apply_socket_options(writer):
    """Aplica opções de socket no nível do SO iguais às do socat."""
    try:
        sock = writer.get_extra_info('socket')
        if sock:
            # Desativa o algoritmo de Nagle (envio imediato sem bufferizar)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # Habilita detecção ativa de Keep-Alive pelo kernel
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Permite reuso rápido do socket pelo SO
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception as e:
        logger.warning(f"Não foi possível aplicar flags de socket: {e}")

async def handle_usr_client(reader, writer):
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do USR-TCP232 (9001): {addr}")
    
    apply_socket_options(writer)
    
    # Se já existir um socket antigo travado, fecha imediatamente para dar lugar ao novo
    if usr_writer is not None:
        logger.warning("Novo login do USR detectado. Encerrando conexão anterior obsoleta...")
        try:
            usr_writer.close()
        except Exception:
            pass

    usr_reader = reader
    usr_writer = writer
    
    try:
        # Fica ativo aguardando a conexão terminar (EOF)
        while True:
            # Mantém a conexão viva sem tentar fazer read concorrente em rotinas do notebook
            await asyncio.sleep(3600)
    except (ConnectionResetError, BrokenPipeError, OSError, asyncio.CancelledError) as e:
        logger.error(f"Conexão USR encerrou (9001): {e}")
    finally:
        logger.info(f"Conexão TCP encerrada do USR-TCP232: {addr}")
        if usr_writer == writer:
            usr_reader = None
            usr_writer = None
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def bridge_forward(reader_src, writer_dst, lock_src, direction_label, is_notebook=False):
    """
    Ponte transparente (pipe): lê os bytes de entrada e repassa imediatamente
    para o socket de destino sem interferir na montagem de pacotes.
    """
    global last_notebook_activity
    try:
        while True:
            # Garante que apenas uma tarefa lê do leitor por vez
            if lock_src:
                async with lock_src:
                    data = await reader_src.read(4096)
            else:
                data = await reader_src.read(4096)

            if not data:
                break  # Conexão encerrada pela outra ponta
            
            if is_notebook:
                last_notebook_activity = time.time()

            logger.debug(f"[{direction_label}] {data.hex()}")
            
            writer_dst.write(data)
            await writer_dst.drain()

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Erro no repasse [{direction_label}]: {e}")

async def handle_notebook_client(reader_nb, writer_nb):
    global last_notebook_activity, usr_reader, usr_writer
    addr = writer_nb.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do Notebook (9002): {addr}")
    
    apply_socket_options(writer_nb)
    
    if not usr_writer or not usr_reader:
        logger.warning("Notebook tentou conectar, mas USR-TCP232 não está conectado na 9001.")
        writer_nb.close()
        await writer_nb.wait_closed()
        return

    async with modbus_lock:
        # Liga as duas pontas em modo Tunnel / Ponte bidirecional
        task_nb_to_usr = asyncio.create_task(
            bridge_forward(reader_nb, usr_writer, None, "NOTEBOOK -> USR", is_notebook=True)
        )
        task_usr_to_nb = asyncio.create_task(
            bridge_forward(usr_reader, writer_nb, usr_reader_lock, "USR -> NOTEBOOK")
        )

        # Roda até que o Notebook ou o USR fechem a conexão
        done, pending = await asyncio.wait(
            [task_nb_to_usr, task_usr_to_nb],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()

    logger.info(f"Conexão TCP encerrada do Notebook (9002): {addr}")
    try:
        writer_nb.close()
        await writer_nb.wait_closed()
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001, reuse_address=True)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002, reuse_address=True)
    
    logger.info("Servidores TCP iniciados nas portas 9001 (USR) e 9002 (Notebook) com flags de SO ativas.")
    yield
    server_9001.close()
    server_9002.close()
    await server_9001.wait_closed()
    await server_9002.wait_closed()

app = FastAPI(title="Modbus Broker", lifespan=lifespan)

@app.get("/health")
async def health():
    in_priority = (time.time() - last_notebook_activity) < NOTEBOOK_PRIORITY_TIMEOUT
    return {
        "status": "online",
        "usr_connected": usr_writer is not None,
        "priority_mode_notebook": in_priority
    }

@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial dos registradores"),
    count: int = Query(10, description="Quantidade de registradores a ler")
):
    global usr_reader, usr_writer
    if (time.time() - last_notebook_activity) < NOTEBOOK_PRIORITY_TIMEOUT:
        logger.info("[BLOQUEIO] Requisição n8n rejeitada: Coleta do Notebook ativa na 9002.")
        raise HTTPException(status_code=503, detail="FieldLogger ocupado em alta prioridade pelo Notebook")

    if not usr_writer or not usr_reader:
        raise HTTPException(status_code=503, detail="USR-TCP232 não conectado na porta 9001")

    raw_payload = bytearray([
        unit,
        0x03,
        (address >> 8) & 0xFF,
        address & 0xFF,
        (count >> 8) & 0xFF,
        count & 0xFF
    ])

    crc = 0xFFFF
    for pos in raw_payload:
        crc ^= pos
        for _ in range(8):
            if (crc & 0x0001) != 0:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    raw_payload.append(crc & 0xFF)
    raw_payload.append((crc >> 8) & 0xFF)

    async with modbus_lock:
        async with usr_reader_lock:
            try:
                usr_writer.write(raw_payload)
                await usr_writer.drain()

                response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
                return {
                    "status": "success",
                    "bytes_hex": response.hex(),
                    "raw_bytes": list(response)
                }
            except asyncio.TimeoutError:
                raise HTTPException(status_code=504, detail="Timeout de resposta do FieldLogger")
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                usr_reader = None
                usr_writer = None
                raise HTTPException(status_code=500, detail=f"Conexão com USR perdida: {e}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
