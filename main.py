import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("modbus_broker")

# Trava global de concorrência: Impede que Notebook (9002) e n8n (5000) enviem comandos Modbus ao mesmo tempo
modbus_lock = asyncio.Lock()

# Ponteiros de stream da conexão TCP persistente vinda do USR-TCP232
usr_reader = None
usr_writer = None

async def handle_usr_client(reader, writer):
    """
    Recebe e mantém a conexão TCP vinda do USR-TCP232 (Porta 9001).
    """
    global usr_reader, usr_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do USR-TCP232 (9001): {addr}")
    usr_reader = reader
    usr_writer = writer
    
    try:
        # Mantém o socket aberto enquanto o USR estiver conectado
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
    """
    Escuta conexões TCP diretas do Notebook (Porta 9002), por exemplo, via software de calibração/Configurador.
    """
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do Notebook (9002): {addr}")
    
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            
            # Adquire a trava para garantir uso exclusivo do canal Modbus
            async with modbus_lock:
                if usr_writer and usr_reader:
                    usr_writer.write(data)
                    await usr_writer.drain()
                    
                    try:
                        response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
                        writer.write(response)
                        await writer.drain()
                    except asyncio.TimeoutError:
                        logger.warning("Timeout: FieldLogger não respondeu à solicitação da porta 9002.")
                else:
                    logger.warning("Tentativa de leitura na 9002 sem USR-TCP232 conectado na 9001.")
    except Exception as e:
        logger.error(f"Erro na ponte do Notebook (9002): {e}")
    finally:
        logger.info(f"Conexão TCP encerrada do Notebook: {addr}")
        writer.close()
        await writer.wait_closed()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicia os servidores TCP paralelos nas portas 9001 e 9002
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

# --- ROTA HTTP PARA O N8N (PORTA 5000) ---
@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial dos registadores"),
    count: int = Query(10, description="Quantidade de registadores a ler")
):
    """
    Endpoint HTTP chamado pelo n8n na porta 5000.
    A requisição é convertida em frame Modbus RTU e enviada ao USR (9001) sob a trava de concorrência.
    """
    if not usr_writer or not usr_reader:
        raise HTTPException(status_code=503, detail="USR-TCP232/FieldLogger não está conectado na porta 9001")

    # Monta a trama Modbus RTU Read Holding Registers (Função 03)
    raw_payload = bytearray([
        unit,
        0x03,
        (address >> 8) & 0xFF,
        address & 0xFF,
        (count >> 8) & 0xFF,
        count & 0xFF
    ])

    # Cálculo de CRC16 Modbus RTU
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

    # Entra na Fila / Trava Global de Concorrência
    async with modbus_lock:
        try:
            usr_writer.write(raw_payload)
            await usr_writer.drain()

            # Aguarda a resposta vinda do FieldLogger via USR
            response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
            
            return {
                "status": "success",
                "bytes_hex": response.hex(),
                "raw_bytes": list(response)
            }
        except asyncio.TimeoutError:
            logger.error("Timeout: FieldLogger não respondeu à solicitação do n8n.")
            raise HTTPException(status_code=504, detail="Timeout de resposta do FieldLogger via RS485")
        except Exception as e:
            logger.error(f"Erro no broker Modbus: {e}")
            raise HTTPException(status_code=500, detail=str(e))
