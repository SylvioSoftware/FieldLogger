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

# Lock global para impedir colisão de pacotes entre n8n e FieldChart
modbus_lock = asyncio.Lock()

usr_reader = None
usr_writer = None

def tune_socket(writer_or_reader):
    """Aplica flags TCP de baixa latência."""
    try:
        sock = writer_or_reader.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception as e:
        logger.warning(f"Erro nas flags do socket: {e}")

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

async def direct_kernel_pipe(reader_src, writer_dst):
    """Ponte de bytes bidirecional para o FieldChart."""
    try:
        while True:
            data = await reader_src.read(4096)
            if not data:
                break
            writer_dst.write(data)
            await writer_dst.drain()
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
        pass

async def handle_notebook_client(reader_nb, writer_nb):
    """Porta 9002: FieldChart / Porta COM Virtual (HW VSP)."""
    global usr_reader, usr_writer
    addr = writer_nb.get_extra_info('peername')
    logger.info(f"[FIELDCHART] Conexão aberta na 9002: {addr}")
    
    tune_socket(reader_nb)
    tune_socket(writer_nb)

    if not usr_writer or not usr_reader:
        logger.error("[FIELDCHART] Rejeitado: USR-TCP232 não conectado na 9001.")
        writer_nb.close()
        await writer_nb.wait_closed()
        return

    # Passagem direta para as leituras pontuais do FieldChart
    async with modbus_lock:
        logger.info("[MULTIPLEX] Canal 9002 ativo para o FieldChart")
        
        task_nb_to_usr = asyncio.create_task(direct_kernel_pipe(reader_nb, usr_writer))
        task_usr_to_nb = asyncio.create_task(direct_kernel_pipe(usr_reader, writer_nb))

        try:
            await asyncio.gather(task_nb_to_usr, task_usr_to_nb, return_exceptions=True)
        finally:
            task_nb_to_usr.cancel()
            task_usr_to_nb.cancel()

    logger.info(f"[FIELDCHART] Conexão encerrada na 9002.")
    try:
        writer_nb.close()
        await writer_nb.wait_closed()
    except Exception:
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001, reuse_address=True)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002, reuse_address=True)
    logger.info("Broker Multiplexador Ativo | 9001 (USR) | 9002 (FieldChart) | 5000 (n8n)")
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
        "usr_connected": usr_writer is not None
    }

@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial"),
    count: int = Query(10, description="Quantidade de registradores")
):
    global usr_reader, usr_writer
    
    if not usr_writer or not usr_reader:
        raise HTTPException(status_code=503, detail="USR-TCP232 não conectado na porta 9001")

    # Monta frame Modbus RTU + CRC16
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

            response = await asyncio.wait_for(usr_reader.read(1024), timeout=2.0)
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
