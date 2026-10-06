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

modbus_lock = asyncio.Lock()

usr_reader = None
usr_writer = None

def tune_socket(writer):
    """Aplica flags TCP para baixa latência e manter a conexão viva."""
    try:
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception as e:
        logger.warning(f"Erro ao aplicar flags no socket: {e}")

async def handle_usr_client(reader, writer):
    """Gerencia a conexão mantida com o módulo USR-TCP232 na porta 9001."""
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"[USR] Dispositivo conectado na porta 9001: {addr}")
    
    tune_socket(writer)

    if usr_writer is not None and not usr_writer.is_closing():
        logger.warning("[USR] Nova conexão recebida. Encerrando anterior...")
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
        logger.info(f"[USR] Dispositivo desconectado da porta 9001: {addr}")
        if usr_writer == writer:
            usr_reader = None
            usr_writer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001, reuse_address=True)
    logger.info("Servidor Modbus REST Ativo na porta 5000 | Aguardando USR na 9001")
    yield
    server_9001.close()
    await server_9001.wait_closed()

app = FastAPI(title="Modbus REST Broker para n8n", lifespan=lifespan)

@app.get("/health")
async def health():
    return {
        "status": "online",
        "usr_connected": usr_writer is not None
    }

@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial dos registradores"),
    count: int = Query(10, description="Quantidade de registradores a ler")
):
    global usr_reader, usr_writer
    
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

    # Cálculo do CRC16 Modbus RTU
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
            # Garante a limpeza de qualquer resíduo anterior no buffer antes de ler
            usr_writer.write(raw_payload)
            await usr_writer.drain()

            response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
            return {
                "status": "success",
                "bytes_hex": response.hex(),
                "raw_bytes": list(response)
            }
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="Timeout de resposta do FieldLogger via USR")
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            usr_reader = None
            usr_writer = None
            raise HTTPException(status_code=500, detail=f"Conexão com USR perdida: {e}")
