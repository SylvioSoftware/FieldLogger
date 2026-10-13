import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("simple_tcp_proxy")

esp32_writer = None
esp32_reader = None

async def proxy_pipe(reader, writer):
    """Encaminha os pacotes TCP bidirecionalmente de forma totalmente transparente."""
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
        pass

async def handle_esp32(reader, writer):
    """Porta 9001: O ESP32 conecta-se aqui de dentro para fora."""
    global esp32_reader, esp32_writer
    addr = writer.get_extra_info('peername')
    logger.info(f"[ESP32] Dispositivo conectado na porta 9001: {addr}")
    
    if esp32_writer and not esp32_writer.is_closing():
        try:
            esp32_writer.close()
        except:
            pass

    esp32_reader = reader
    esp32_writer = writer

    try:
        await writer.wait_closed()
    except Exception:
        pass
    finally:
        logger.info(f"[ESP32] Desconectado da porta 9001: {addr}")
        if esp32_writer == writer:
            esp32_reader = None
            esp32_writer = None

async def handle_client(client_reader, client_writer):
    """Porta 9002: Notebook (Modbus Poll) ou n8n conectam-se aqui."""
    global esp32_reader, esp32_writer
    client_addr = client_writer.get_extra_info('peername')
    logger.info(f"[CLIENTE] Aplicação conectada na porta 9002: {client_addr}")

    if not esp32_reader or not esp32_writer or esp32_writer.is_closing():
        logger.error("[CLIENTE] Rejeitado: ESP32 não está conectado na porta 9001.")
        client_writer.close()
        await client_writer.wait_closed()
        return

    # Abre o túnel transparente entre o cliente e o ESP32
    t1 = asyncio.create_task(proxy_pipe(client_reader, esp32_writer))
    t2 = asyncio.create_task(proxy_pipe(esp32_reader, client_writer))

    try:
        await asyncio.gather(t1, t2, return_exceptions=True)
    finally:
        t1.cancel()
        t2.cancel()
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except:
            pass
        logger.info(f"[CLIENTE] Conexão encerrada para: {client_addr}")

async def main():
    server_esp32 = await asyncio.start_server(handle_esp32, '0.0.0.0', 9001, reuse_address=True)
    server_client = await asyncio.start_server(handle_client, '0.0.0.0', 9002, reuse_address=True)
    
    logger.info("Proxy TCP simples iniciado | ESP32 na 9001 | Clientes na 9002")
    
    async with server_esp32, server_client:
        await asyncio.gather(
            server_esp32.serve_forever(),
            server_client.serve_forever()
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Proxy encerrado.")
