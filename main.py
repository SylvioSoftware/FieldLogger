import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("modbus_broker")

modbus_lock = asyncio.Lock()

# Armazena o stream do USR-TCP232
usr_reader = None
usr_writer = None

async def handle_usr_client(reader, writer):
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do USR-TCP232 (9001): {addr}")
    usr_reader = reader
    usr_writer = writer
    
    try:
        # Mantém a conexão viva sem bloquear leituras externas
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Conexão USR (9001) perdida: {e}")
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
                break
            
            async with modbus_lock:
                if usr_writer and usr_reader:
                    # Envia comando ao USR
                    usr_writer.write(data)
                    await usr_writer.drain()
                    
                    # Lê resposta com timeout de 3s (útil se o FieldLogger estiver desligado)
                    try:
                        response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
                        writer.write(response)
                        await writer.drain()
                    except asyncio.TimeoutError:
                        logger.warning("Timeout aguardando resposta do FieldLogger (equipamento desligado/sem resposta RS485).")
                else:
                    logger.warning("Tentativa de leitura sem USR-TCP232 conectado na 9001.")
    except Exception as e:
        logger.error(f"Erro na ponte do Notebook (9002): {e}")
    finally:
        logger.info(f"Conexão TCP encerrada do Notebook: {addr}")
        writer.close()
        await writer.wait_closed()

@asynccontextmanager
async def lifespan(app: FastAPI):
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002)
    
    logger.info("Servidores TCP ativos nas portas 9001 (USR) e 9002 (Notebook)")
    
    yield
    
    server_9001.close()
    server_9002.close()
    await server_9001.wait_closed()
    await server_9002.wait_closed()

app = FastAPI(title="Modbus Broker & Gateway", lifespan=lifespan)

@app.get("/health")
async def health():
    return {
        "status": "online",
        "usr_connected": usr_writer is not None
    }
