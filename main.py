import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

# LOG VERBOSO ATIVADO: Mostra todas as trocas de bytes e chamadas internas
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("modbus_broker")

# Trava global de concorrência para evitar colisão entre a porta 9002 (Notebook) e 5000 (n8n)
modbus_lock = asyncio.Lock()

usr_reader = None
usr_writer = None

async def handle_usr_client(reader, writer):
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do USR-TCP232 (9001): {addr}")
    usr_reader = reader
    usr_writer = writer
    
    try:
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Exceção na conexão USR (9001): {e}", exc_info=True)
    finally:
        logger.info(f"Conexão TCP encerrada do USR-TCP232: {addr}")
        usr_reader = None
        usr_writer = None
        writer.close()
        await writer.wait_closed()

async def handle_notebook_client(reader, writer):
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do Notebook (9002): {addr}")
    
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                logger.debug("Notebook desconectou a transmissão (0 bytes lidos).")
                break
            
            logger.debug(f"[9002 -> USR] Bytes recebidos do Notebook: {data.hex()}")
            
            async with modbus_lock:
                if usr_writer and usr_reader:
                    usr_writer.write(data)
                    await usr_writer.drain()
                    logger.debug("[9002 -> USR] Dados enviados ao USR-TCP232. Aguardando resposta...")
                    
                    try:
                        response = await asyncio.wait_for(usr_reader.read(4096), timeout=5.0)
                        logger.debug(f"[USR -> 9002] Resposta recebida do FieldLogger: {response.hex()}")
                        
                        writer.write(response)
                        await writer.drain()
                        logger.debug("[USR -> 9002] Resposta entregue ao Notebook.")
                    except asyncio.TimeoutError:
                        logger.warning("Timeout aguardando resposta do FieldLogger na porta 9001.")
                else:
                    logger.warning("Solicitação na porta 9002 rejeitada: USR-TCP232 (9001) não está conectado.")
    except Exception as e:
        logger.error(f"Erro no manuseio da porta 9002: {e}", exc_info=True)
    finally:
        logger.info(f"Conexão TCP encerrada do Notebook (9002): {addr}")
        writer.close()
        await writer.wait_closed()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando servidores TCP nas portas 9001 e 9002...")
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002)
    
    logger.info("Servidores TCP ativos e prontos nas portas 9001 e 9002.")
    yield
    server_9001.close()
    server_9002.close()
    await server_9001.wait_closed()
    await server_9002.wait_closed()

# Instância pública do FastAPI exigida pelo Uvicorn/ASGI
app = FastAPI(title="Modbus Broker & Gateway", lifespan=lifespan)

@app.get("/health")
async def health():
    return {
        "status": "online",
        "usr_connected": usr_writer is not None
    }

@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial dos registadores"),
    count: int = Query(10, description="Quantidade de registadores a ler")
):
    if not usr_writer or not usr_reader:
        raise HTTPException(status_code=503, detail="USR-TCP232 / FieldLogger não conectado na porta 9001")

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

    logger.debug(f"[n8n -> USR] Payload Modbus RTU enviado: {raw_payload.hex()}")

    async with modbus_lock:
        try:
            usr_writer.write(raw_payload)
            await usr_writer.drain()

            response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
            logger.debug(f"[USR -> n8n] Resposta Modbus RTU recebida: {response.hex()}")
            
            return {
                "status": "success",
                "bytes_hex": response.hex(),
                "raw_bytes": list(response)
            }
        except asyncio.TimeoutError:
            logger.error("Timeout aguardando resposta do FieldLogger para a chamada do n8n.")
            raise HTTPException(status_code=504, detail="Timeout de resposta do FieldLogger")
        except Exception as e:
            logger.error(f"Erro ao processar chamada HTTP do n8n: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
