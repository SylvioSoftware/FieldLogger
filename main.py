import asyncio
from fastapi import FastAPI, HTTPException
from pymodbus.client import AsyncModbusTcpClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("modbus_broker")

app = FastAPI(title="Modbus Broker & Gateway")

# Trava global para evitar colisão entre requisições do n8n e do Notebook
modbus_lock = asyncio.ActionLock() if hasattr(asyncio, 'ActionLock') else asyncio.Lock()

# Configuração do USR-TCP232 / FieldLogger
# Se o USR for cliente, ele se conecta no servidor local na porta 9001
FIELDLOGGER_HOST = "127.0.0.1" 
FIELDLOGGER_PORT = 9001

@app.get("/health")
async def health():
    return {"status": "online"}

@app.get("/read")
async def read_fieldlogger():
    async with modbus_lock:
        try:
            # Conecta ao socket TCP do FieldLogger/USR
            client = AsyncModbusTcpClient(FIELDLOGGER_HOST, port=FIELDLOGGER_PORT, timeout=2)
            connected = await client.connect()
            
            if not connected:
                raise HTTPException(status_code=503, detail="Não foi possível conectar ao conversor USR")

            # Leitura dos registradores das temperaturas (ajuste os endereços conforme seu mapa de registradores)
            # Exemplo: Leitura de 16 registradores de Holding (30001 / 40001)
            response = await client.read_holding_registers(address=0, count=16, slave=1)
            client.close()

            if response.isError():
                raise HTTPException(status_code=500, detail=f"Erro Modbus: {response}")

            regs = response.registers

            # Exemplo de conversão simplificada de registradores para valores Reais/Float (10x para escala ou divisão por 10)
            # Ajustaremos a conversão exata de 32-bit Float conforme o mapa do seu FieldLogger
            dados_lidos = {
                "pt100": regs[0] / 10.0,
                "t11": regs[1] / 10.0,
                "t12": regs[2] / 10.0,
                "t13": regs[3] / 10.0,
                "t14": regs[4] / 10.0,
                "t15": regs[5] / 10.0,
                "t16": regs[6] / 10.0,
                "t17": regs[7] / 10.0,
                "p21": regs[8] / 10.0,
                "t21": regs[9] / 10.0,
                "t22": regs[10] / 10.0,
                "t23": regs[11] / 10.0,
                "t24": regs[12] / 10.0,
                "t25": regs[13] / 10.0,
                "t26": regs[14] / 10.0,
                "t27": regs[15] / 10.0,
            }

            return dados_lidos

        except Exception as e:
            logger.error(f"Erro ao ler Modbus: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
