import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pymodbus.client import AsyncModbusTcpClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("modbus_broker")

# Trava global para arbitrar acesso ao barramento e evitar colisão
modbus_lock = asyncio.Lock()

# Armazena a última conexão TCP ativa do USR-TCP232 / FieldLogger
active_fieldlogger_stream = None

async def handle_usr_client(reader, writer):
    global active_fieldlogger_stream
    addr = writer.get_extra_info('peername')
    logger.info(f" Conexão TCP estabelecida do USR-TCP232 (9001): {addr}")
    active_fieldlogger_stream = (reader, writer)
    
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
    except Exception as e:
        logger.error(f"Erro na conexão USR (9001): {e}")
    finally:
        logger.info(f"Conexão TCP encerrada do USR-TCP232: {addr}")
        active_fieldlogger_stream = None
        writer.close()
        await writer.wait_closed()

async def handle_notebook_client(reader, writer):
    addr = writer.get_extra_info('peername')
    logger.info(f" Conexão TCP estabelecida do Notebook (9002): {addr}")
    
    try:
        while True:
            # Recebe o comando do Notebook
            data = await reader.read(1024)
            if not data:
                break
            
            # Repassa ao USR-TCP232 usando a trava para não colidir com o n8n
            async with modbus_lock:
                if active_fieldlogger_stream:
                    usr_reader, usr_writer = active_fieldlogger_stream
                    usr_writer.write(data)
                    await usr_writer.drain()
                    
                    response = await usr_reader.read(1024)
                    writer.write(response)
                    await writer.drain()
    except Exception as e:
        logger.error(f"Erro na ponte do Notebook (9002): {e}")
    finally:
        logger.info(f"Conexão TCP encerrada do Notebook: {addr}")
        writer.close()
        await writer.wait_closed()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicia os servidores TCP em background ao ligar o container
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001)
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002)
    
    logger.info(" Servidores TCP ativos nas portas 9001 (USR) e 9002 (Notebook)")
    
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
        "usr_connected": active_fieldlogger_stream is not None
    }

@app.get("/read")
async def read_fieldlogger():
    if not active_fieldlogger_stream:
        raise HTTPException(status_code=503, detail="USR-TCP232 não está conectado na porta 9001")
    
    async with modbus_lock:
        try:
            # Exemplo de leitura via soquete ativo
            usr_reader, usr_writer = active_fieldlogger_stream
            
            # Trama de leitura Modbus RTU / TCP conforme configuração do seu FieldLogger
            # (Envia requisição e recebe a resposta)
            # Para teste rápido de resposta do broker:
            return {"status": "ok", "message": "Dispositivo conectado e pronto para polling"}
            
        except Exception as e:
            logger.error(f"Erro ao ler Modbus: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
