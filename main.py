import asyncio
import logging
import socket
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("modbus_proxy")

# Lock global para gerenciar acesso à RS485
modbus_lock = asyncio.Lock()

usr_reader = None
usr_writer = None

def tune_socket(writer):
    """Aplica flags TCP para detectar conexões mortas e reduzir latência."""
    try:
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Configurações de Keepalive no SO para matar sockets zumbis rapidamente
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)  # 5s ocioso
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2) # 2s intervalo
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)   # 3 tentativas
    except Exception as e:
        logger.warning(f"Erro ao aplicar flags no socket: {e}")

async def handle_usr_client(reader, writer):
    """Porta 9001: Módulo USR-TCP232 (FieldLogger)."""
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"[USR] Conectado na porta 9001: {addr}")
    
    tune_socket(writer)

    if usr_writer is not None and not usr_writer.is_closing():
        try:
            usr_writer.close()
        except Exception:
            pass

    usr_reader = reader
    usr_writer = writer

    try:
        await writer.wait_closed()
    except (ConnectionResetError, BrokenPipeError, OSError, asyncio.CancelledError):
        pass
    finally:
        logger.info(f"[USR] Desconectado da porta 9001: {addr}")
        if usr_writer == writer:
            usr_reader = None
            usr_writer = None

async def direct_kernel_pipe(reader_src, writer_dst, timeout_sec=5.0):
    """Passa bytes de um lado para o outro com timeout para matar conexões mortas."""
    try:
        while True:
            # Se passar 5 segundos sem nenhum pacote do FieldChart, assume inatividade/queda
            data = await asyncio.wait_for(reader_src.read(4096), timeout=timeout_sec)
            if not data:
                break
            writer_dst.write(data)
            await writer_dst.drain()
    except (asyncio.TimeoutError, asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
        pass

async def handle_notebook_client(reader_nb, writer_nb):
    """Porta 9002: FieldChart / HW VSP / PC."""
    global usr_reader, usr_writer
    addr = writer_nb.get_extra_info('peername')
    logger.info(f"[FIELDCHART] Conexão aberta na porta 9002: {addr}")
    
    # Passa APENAS o writer para o tune_socket
    tune_socket(writer_nb)

    if not usr_writer or not usr_reader:
        logger.error("[FIELDCHART] Rejeitado: USR-TCP232 não conectado na 9001.")
        writer_nb.close()
        await writer_nb.wait_closed()
        return

    # Passagem transparente de leituras pontuais com Timeout automático
    async with modbus_lock:
        logger.info("[MULTIPLEX] Canal 9002 ativo para o FieldChart")
        
        task_nb_to_usr = asyncio.create_task(direct_kernel_pipe(reader_nb, usr_writer, timeout_sec=5.0))
        task_usr_to_nb = asyncio.create_task(direct_kernel_pipe(usr_reader, writer_nb, timeout_sec=5.0))

        try:
            await asyncio.gather(task_nb_to_usr, task_usr_to_nb, return_exceptions=True)
        finally:
            task_nb_to_usr.cancel()
            task_usr_to_nb.cancel()

    logger.info(f"[FIELDCHART] Conexão encerrada/liberada na 9002.")
    try:
        writer_nb.close()
        await writer_nb.wait_closed()
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001, reuse_address=True)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002, reuse_address=True)
    logger.info("Broker Multiplexador Robusto | 9001 (USR) | 9002 (FieldChart) | 5000 (n8n)")
    yield
    server_9001.close()
    server_9002.close()
    await server_9001.wait_closed()
    await server_9002.wait_closed()

app = FastAPI(title="Modbus Multiplexer Broker", lifespan=lifespan)

@app.get("/health")
async def health():
    return {
        "status": "online",
        "usr_connected": usr_writer is not None,
        "lock_status": "locked" if modbus_lock.locked() else "free"
    }

@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1),
    address: int = Query(0),
    count: int = Query(10)
):
    global usr_reader, usr_writer
    
    if not usr_writer or not usr_reader:
        raise HTTPException(status_code=503, detail="USR-TCP232 não conectado na porta 9001")

    # Se a porta 9002 estiver ativa no momento, rejeita na hora para o n8n não ficar preso
    if modbus_lock.locked():
        raise HTTPException(status_code=503, detail="Barramento ocupado pela porta 9002")

    raw_payload = bytearray([
        unit, 0x03,
        (address >> 8) & 0xFF, address & 0xFF,
        (count >> 8) & 0xFF, count & 0xFF
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
        try:
            usr_writer.write(raw_payload)
            await usr_writer.drain()

            response = await asyncio.wait_for(usr_reader.read(1024), timeout=2.5)
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
