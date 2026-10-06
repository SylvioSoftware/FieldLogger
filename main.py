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

def tune_socket(writer_or_reader):
    """Aplica flags TCP diretamente no SO para garantir baixa latência e máximo throughput."""
    try:
        sock = writer_or_reader.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception as e:
        logger.warning(f"Erro ao aplicar configurações no socket: {e}")

async def handle_usr_client(reader, writer):
    """Gerencia a conexão mantida com o módulo USR-TCP232 na porta 9001."""
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"[USR] Dispositivo conectado na porta 9001: {addr}")
    
    tune_socket(writer)

    if usr_writer is not None and not usr_writer.is_closing():
        logger.warning("[USR] Nova conexão recebida no USR. Encerrando anterior...")
        try:
            usr_writer.close()
        except Exception:
            pass

    usr_reader = reader
    usr_writer = writer

    try:
        # Mantém a conexão ativa no loop sem bloquear o ponteiro de leitura do StreamReader
        await writer.wait_closed()
    except (ConnectionResetError, BrokenPipeError, OSError, asyncio.CancelledError):
        pass
    finally:
        logger.info(f"[USR] Dispositivo desconectado da porta 9001: {addr}")
        if usr_writer == writer:
            usr_reader = None
            usr_writer = None

async def direct_kernel_pipe(reader_src, writer_dst, label, is_notebook=False):
    """
    Ponte de transmissão de dados brutos (Direct Pipe nível de socket).
    Simula o comportamento do socat repassando pacotes bidirecionalmente sem parsing.
    """
    global last_notebook_activity
    try:
        while True:
            data = await reader_src.read(16384)  # Chunks de 16KB para aguentar rajadas pesadas
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
    Atende conexões na porta 9002 (Eltima / FieldLogger Config).
    Conecta o socket 9002 ao socket 9001 em modo transparente e bloqueia o n8n temporariamente.
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

    # Adquire o lock para evitar que requisições REST do n8n misturem bytes na transmissão Modbus RTU
    async with modbus_lock:
        logger.info("[MODO TUNNEL] Mapeando tráfego direto FieldLogger Config <-> USR-TCP232")
        
        task_nb_to_usr = asyncio.create_task(
            direct_kernel_pipe(reader_nb, usr_writer, "NOTEBOOK -> USR", is_notebook=True)
        )
        task_usr_to_nb = asyncio.create_task(
            direct_kernel_pipe(usr_reader, writer_nb, "USR -> NOTEBOOK")
        )

        try:
            # Aguarda o encerramento da comunicação por qualquer um dos lados
            await asyncio.gather(task_nb_to_usr, task_usr_to_nb, return_exceptions=True)
        except Exception as e:
            logger.error(f"[TUNNEL] Exceção durante a ponte: {e}")
        finally:
            task_nb_to_usr.cancel()
            task_usr_to_nb.cancel()
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
    
    # Se o FieldLogger Config estiver a realizar download de dados ou terminou há menos de 10 segundos
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
