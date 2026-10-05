async def handle_notebook_client(reader, writer):
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão TCP estabelecida do Notebook (9002): {addr}")
    
    try:
        while True:
            # Lê o comando enviado pelo programa do FieldLogger / HW VSP
            data = await reader.read(1024)
            if not data:
                break
            
            async with modbus_lock:
                if usr_writer and usr_reader:
                    # Envia os bytes recebidos do Notebook direto para o USR-TCP232
                    usr_writer.write(data)
                    await usr_writer.drain()
                    
                    try:
                        # Aguarda a resposta do FieldLogger (Timeout de 5s para coleta de memória)
                        response = await asyncio.wait_for(usr_reader.read(4096), timeout=5.0)
                        
                        # Devolve a resposta do FieldLogger de volta para o Notebook (HW VSP)
                        writer.write(response)
                        await writer.drain()
                    except asyncio.TimeoutError:
                        logger.warning("Timeout aguardando resposta do FieldLogger (9001 -> 9002).")
                else:
                    logger.warning("Notebook tentou comunicação na 9002, mas USR-TCP232 está desconectado na 9001.")
    except Exception as e:
        logger.error(f"Erro na ponte do Notebook (9002): {e}")
    finally:
        logger.info(f"Conexão TCP encerrada do Notebook: {addr}")
        writer.close()
        await writer.wait_closed()
