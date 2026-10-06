import asyncio
import logging
import socket
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("modbus_proxy")

modbus_lock = asyncio.Lock()

usr_reader = None
usr_writer = None

last_notebook_activity = 0
NOTEBOOK_PRIORITY_TIMEOUT = 10.0  # Tempo de tolerância pós-download (em segundos)

def tune_socket(writer):
    """Aplica flags de socket direto no Kernel do SO para simular a performance do socat."""
    try:
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception as e:
        logger.warning(f"Erro ao ajustar flags de socket: {e}")

async def handle_usr_client(reader, writer):
    """Gerencia a conexão mantida com o módulo USR-TCP232 na porta 9001."""
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"[USR] Dispositivo conectado: {addr}")
    
    tune_socket(writer)

    if usr_writer is not None:
        logger.warning("[USR] Nova conexão recebida. Encerrando conexão anterior...")
        try:
            usr_writer.close()
        except Exception:
            pass

    usr_reader = reader
    usr_writer = writer

    try:
        # Mantém o socket ativo
        while not writer.is_closing():
            await asyncio.sleep(3600)
    except (ConnectionResetError, BrokenPipeError, OSError, asyncio.CancelledError):
        pass
    finally:
        logger.info(f"[USR] Dispositivo desconectado: {addr}")
        if usr_writer == writer:
            usr_reader = None
            usr_writer = None
        try:
            writer.close()
        except Exception:
            pass

async def direct_kernel_pipe(reader_src, writer_dst, label, is_notebook=False):
    """
    Ponte de altíssima velocidade (Direct Pipe).
    Lê blocos brutos em nível de socket sem interferência do event-loop do Python.
    """
    global last_notebook_activity
    try:
        while True:
            # Chunk grande para suportar rajadas massivas da memória flash
            data = await reader_src.read(16384)
            if not data:
                break
            
            if is_notebook:
                last_notebook_activity = time.time()

            writer_dst.write(data)
            await writer_dst.drain()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.debug(f"Ponte [{label}] encerrada: {e}")

async def handle_notebook_client(reader_nb, writer_nb):
    """
    Quando o FieldLogger Config conecta na porta 9002, o Python assume o modo 'socat':
    Bloqueia chamadas HTTP e faz o repasse transparente de dados na máxima velocidade.
    """
    global last_notebook_activity, usr_reader, usr_writer
    addr = writer_nb.get_extra_info('peername')
    logger.info(f"[NOTEBOOK] Conexão iniciada na porta 9002 (Modo SOCAT Automático Ativado): {addr}")
    
    tune_socket(writer_nb)

    if not usr_writer or not usr_reader:
        logger.error("[NOTEBOOK] Rejeitado: USR-TCP232 não está conectado na porta 9001.")
        writer_nb.close()
        await writer_nb.wait_closed()
        return

    # Bloqueia qualquer chamada REST enquanto o Notebook/FieldLogger Config estiver operando
    async with modbus_lock:
        logger.info("[MODO TUNNEL] Mapeando tráfego direto FieldLogger Config <-> USR-TCP232")
        
        task_nb_to_usr = asyncio.create_task(
            direct_kernel_pipe(reader_nb, usr_writer, "NOTEBOOK -> USR", is_notebook=True)
        )
        task_usr_to_nb = asyncio.create_task(
            direct_kernel_pipe(usr_reader, writer_nb, "USR -> NOTEBOOK")
        )

        # Aguarda qualquer uma das pontas encerrar o download ou fechar a porta
        done, pending = await asyncio.wait(
            [task_nb_to_usr, task_usr_to_nb],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()

        last_notebook_activity = time.time()

    logger.info(f"[NOTEBOOK] Operação concluída. Fechando ponte 9002. Reassumindo modo REST/n8n.")
    try:
        writer_nb.close()
        await writer_nb.wait_closed()
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001, reuse_address=True)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002, reuse_address=True)
    
    logger.info("Servidor Proxy Modbus Ativo: Porta 9001 (USR) | Porta 9002 (FieldLogger Config / Socat Auto)")
    yield
    server_9001.close()
    server_9002.close()
    await server_9001.wait_closed()
    await server_9002.wait_closed()

app = FastAPI(title="Modbus Automatic Tunnel Broker", lifespan=lifespan)

@app.get("/health")
async def health():
    in_priority = (time.time() - last_notebook_activity) < NOTEBOOK_PRIORITY_TIMEOUT
    return {
        "status": "online",
        "usr_connected": usr_writer is not None,
        "notebook_active_or_cooling": in_priority
    }

@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial dos registradores"),
    count: int = Query(10, description="Quantidade de registradores a ler")
):
    global usr_reader, usr_writer
    
    # Se o FieldLogger Config estiver baixando dados ou finalizou a menos de 10 segundos
    if (time.time() - last_notebook_activity) < NOTEBOOK_PRIORITY_TIMEOUT:
        raise HTTPException(
            status_code=503, 
            detail="FieldLogger ocupado em alta prioridade realizando descarga/configuração."
        )

    if not usr_writer or not usr_reader:
        raise HTTPException(status_code=503, detail="USR-TCP232 não conectado na porta 9001")

    # Monta o frame Modbus RTU puramente manual
    raw_payload = bytearray([
        unit,
        0x03,
        (address >> 8) & 0xFF,
        address & 0xFF,
        (count >> 8) & 0xFF,
        count & 0xFF
    ])

    # Cálculo do CRC16 Modbus
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
        try:
            # Limpa qualquer resíduo do buffer antes de enviar
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
