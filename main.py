# Importa o módulo de E/S assíncrona nativo do Python (gerencia tarefas concorrentes sem threads)
import asyncio
# Importa o módulo padrão para geração de logs e mensagens de depuração
import logging
# Importa o módulo select para monitoramento de estados de conectividade e I/O de sockets
import select
# Importa o módulo time para medição e controle de timestamps
import time
# Importa o utilitário para gerenciamento do ciclo de vida da aplicação FastAPI
from contextlib import asynccontextmanager
# Importa os componentes do framework FastAPI para construção dos endpoints HTTP e respostas de erro
from fastapi import FastAPI, HTTPException, Query

# Configura o formato e o nível global de registo de logs da aplicação (nível DEBUG exibe tudo)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
# Cria uma instância dedicada do registador de log com o identificador 'modbus_broker'
logger = logging.getLogger("modbus_broker")

# Instancia uma trava de exclusão mútua assíncrona (Mutex) para evitar acessos simultâneos ao USR
modbus_lock = asyncio.Lock()
# Inicializa a variável que guarda o último timestamp em que o Notebook enviou dados
last_notebook_activity = 0
# Define a janela de tempo (em segundos) que o Notebook terá prioridade absoluta na linha serial
NOTEBOOK_PRIORITY_TIMEOUT = 5.0

# Declara a variável global para armazenar o objeto leitor da conexão com o USR-TCP232
usr_reader = None
# Declara a variável global para armazenar o objeto escritor da conexão com o USR-TCP232
usr_writer = None

# Define a função de callback assíncrona que lida com o cliente USR-TCP232 (porta 9001)
async def handle_usr_client(reader, writer):
    # Indica o uso das variáveis globais para manter o socket ativo durante o ciclo da aplicação
    global usr_reader, usr_writer
    # Extrai o endereço IP e porta de origem do equipamento cliente conectado
    addr = writer.get_extra_info('peername')
    # Registra no log que o equipamento USR-TCP232 established a conexão TCP na porta 9001
    logger.info(f"Conexão TCP estabelecida do USR-TCP232 (9001): {addr}")
    # Atribui o fluxo de leitura do socket à variável global
    usr_reader = reader
    # Atribui o fluxo de escrita do socket à variável global
    usr_writer = writer
    
    # Bloco para manter a conexão aberta indefinidamente
    try:
        # Loop infinito para manter a corrotina viva enquanto a conexão persistir
        while True:
            # Pausa a execução da corrotina por 1 hora sem bloquear a thread principal
            await asyncio.sleep(3600)
    # Captura eventuais exceções de rede ou desconexão do hardware
    except Exception as e:
        # Registra a mensagem de erro no log caso a conexão caia
        logger.error(f"Exceção na conexão USR (9001): {e}")
    # Bloco executado sempre que a conexão for encerrada
    finally:
        # Registra no log o encerramento da conexão TCP do USR
        logger.info(f"Conexão TCP encerrada do USR-TCP232: {addr}")
        # Limpa a variável global do leitor
        usr_reader = None
        # Limpa a variável global do escritor
        usr_writer = None
        # Solicita o fechamento do socket do cliente
        writer.close()
        # Aguarda a confirmação de encerramento do socket pelo sistema operacional
        await writer.wait_closed()

# Define a função de callback assíncrona que lida com o software do Notebook (porta 9002)
async def handle_notebook_client(reader, writer):
    # Indica o uso da variável global que controla o tempo de atividade do Notebook
    global last_notebook_activity
    # Obtém as informações do IP e porta do Notebook conectado
    addr = writer.get_extra_info('peername')
    # Registra no log que o Notebook estabeleceu conexão na porta 9002
    logger.info(f"Conexão TCP estabelecida do Notebook (9002): {addr}")
    
    # Bloco para processamento das requisições de coleta do FieldLogger Config
    try:
        # Loop continuo de escuta enquanto o Notebook transmitir dados
        while True:
            # Lê até 4096 bytes recebidos do socket do Notebook
            data = await reader.read(4096)
            # Se não houver dados retornados (0 bytes), o cliente fechou o socket
            if not data:
                # Interrompe o loop de leitura
                break
            
            # Atualiza o timestamp atual marcando atividade ativa no Notebook
            last_notebook_activity = time.time()
            # Registra no log o pacote hexadecimal enviado pelo Notebook
            logger.debug(f"[NOTEBOOK -> USR] {data.hex()}")
            
            # Adquire a trava mutex para garantir acesso exclusivo à conexão com o USR
            async with modbus_lock:
                # Verifica se a conexão com o hardware USR-TCP232 está ativa
                if usr_writer and usr_reader:
                    # PURGA DE BUFFER: Descarta pacotes antigos retidos no socket antes de novo envio
                    try:
                        while True:
                            # Tenta ler dados residuais no buffer com tempo limite de 10ms
                            stale_data = await asyncio.wait_for(usr_reader.read(1024), timeout=0.01)
                            if stale_data:
                                # Registra no log o descarte de dados residuais
                                logger.debug(f"[PURGA BUFFER USR] Descartado: {stale_data.hex()}")
                            else:
                                break
                    # Quando o buffer do socket está totalmente limpo, estoura o timeout de 10ms
                    except asyncio.TimeoutError:
                        pass

                    # Bloco protegido de transmissão para o USR tratando queda de rede física
                    try:
                        # Envia a nova sequência de bytes diretamente para o socket do USR-TCP232
                        usr_writer.write(data)
                        # Força o esvaziamento da fila do socket enviando todos os dados pela rede
                        await usr_writer.drain()
                        
                        # Aguarda o retorno da resposta correspondente com timeout de 5 segundos
                        response = await asyncio.wait_for(usr_reader.read(4096), timeout=5.0)
                        # Registra no log a resposta hexadecimal devolvida pelo FieldLogger
                        logger.debug(f"[USR -> NOTEBOOK] {response.hex()}")
                        # Escreve a resposta de volta no socket do Notebook
                        writer.write(response)
                        # Garante o envio imediato dos bytes de resposta para o Notebook
                        await writer.drain()
                    # Trata o estouro do tempo limite de resposta do hardware
                    except asyncio.TimeoutError:
                        # Registra um aviso no log informando o estouro de tempo limite
                        logger.warning("Timeout aguardando resposta do FieldLogger para a porta 9002.")
                    # Captura queda abrupta de conexão TCP do lado do USR sem derrubar a porta 9002
                    except (ConnectionResetError, BrokenPipeError, OSError) as net_err:
                        # Registra no log a desconexão física do USR
                        logger.error(f"USR-TCP232 desconectou durante a transmissão: {net_err}")
                # Se o USR não estiver conectado no broker
                else:
                    # Registra aviso informando ausência de conexão na porta 9001
                    logger.warning("Notebook enviou dados, mas o USR-TCP232 não está conectado na 9001.")
    # Trata exceções não previstas no ciclo do socket do Notebook
    except Exception as e:
        # Exibe o erro ocorrido na porta 9002
        logger.error(f"Erro no manuseio da porta 9002 (Notebook): {e}")
    # Bloco executado ao término da comunicação ou desconexão
    finally:
        # Registra a desconexão da porta 9002 no log
        logger.info(f"Conexão TCP encerrada do Notebook (9002): {addr}")
        # Solicita o encerramento do socket com o Notebook
        writer.close()
        # Aguarda a liberação dos recursos do socket pelo sistema
        await writer.wait_closed()

# Define o gerenciador de contexto assíncrono para startup e shutdown da aplicação
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicia o servidor TCP escutando na porta 9001 para receber o USR-TCP232
    server_9001 = await asyncio.start_server(handle_usr_client, '0.0.0.0', 9001)
    # Inicia o servidor TCP escutando na porta 9002 para receber o FieldLogger Config
    server_9002 = await asyncio.start_server(handle_notebook_client, '0.0.0.0', 9002)
    
    # Registra no log que ambos os servidores TCP foram inicializados com sucesso
    logger.info("Servidores TCP iniciados nas portas 9001 (USR) e 9002 (Notebook).")
    # Cede o controle para o FastAPI executar o servidor Web/HTTP
    yield
    # Encerra a escuta de novas conexões na porta 9001 ao fechar a aplicação
    server_9001.close()
    # Encerra a escuta de novas conexões na porta 9002 ao fechar a aplicação
    server_9002.close()
    # Aguarda o término da limpeza dos sockets da porta 9001
    await server_9001.wait_closed()
    # Aguarda o término da limpeza dos sockets da porta 9002
    await server_9002.wait_closed()

# Instancia a aplicação FastAPI (a variável 'app' é exigida pelo Uvicorn/Easypanel)
app = FastAPI(title="Modbus Broker", lifespan=lifespan)

# Define a rota de diagnóstico de saúde da aplicação
@app.get("/health")
async def health():
    # Calcula se a aplicação está dentro da janela de prioridade exclusiva do Notebook
    in_priority = (time.time() - last_notebook_activity) < NOTEBOOK_PRIORITY_TIMEOUT
    # Retorna o dicionário serializado automaticamente em JSON com o status do sistema
    return {
        "status": "online",
        "usr_connected": usr_writer is not None,
        "priority_mode_notebook": in_priority
    }

# Define o endpoint HTTP utilizado pelo n8n na porta 5000 para leitura de registradores Modbus
@app.get("/read_holding_registers")
async def read_holding_registers(
    unit: int = Query(1, description="ID Modbus do escravo"),
    address: int = Query(0, description="Endereço inicial dos registadores"),
    count: int = Query(10, description="Quantidade de registadores a ler")
):
    # Verifica se o Notebook realizou comunicação dentro da janela dos últimos 5 segundos
    if (time.time() - last_notebook_activity) < NOTEBOOK_PRIORITY_TIMEOUT:
        # Registra no log o bloqueio/rejeição da requisição vinda do n8n
        logger.info("[BLOQUEIO] Requisição do n8n rejeitada/bloqueada: Coleta do Notebook ativa na 9002.")
        # Lança exceção HTTP 503 (Serviço Indisponível) liberando o n8n sem interferir na coleta
        raise HTTPException(status_code=503, detail="FieldLogger ocupado em alta prioridade pelo Notebook")

    # Verifica se a conexão física com o USR-TCP232 está ativa antes de tentar enviar comando
    if not usr_writer or not usr_reader:
        # Lança exceção HTTP 503 informando que o equipamento está desconectado
        raise HTTPException(status_code=503, detail="USR-TCP232 não conectado na porta 9001")

    # Constrói o vetor de bytes (bytearray) contendo o cabeçalho do comando Modbus RTU (Função 0x03)
    raw_payload = bytearray([
        unit,                        # Endereço ID do escravo Modbus
        0x03,                        # Código da Função Modbus: Read Holding Registers
        (address >> 8) & 0xFF,       # Byte mais significativo (MSB) do endereço inicial
        address & 0xFF,              # Byte menos significativo (LSB) do endereço inicial
        (count >> 8) & 0xFF,         # Byte mais significativo (MSB) da quantidade de registradores
        count & 0xFF                 # Byte menos significativo (LSB) da quantidade de registradores
    ])

    # Inicializa o valor base de 16 bits para cálculo de checksum CRC16 Modbus
    crc = 0xFFFF
    # Percorre cada byte montado no payload para efetuar o cálculo de redundância cíclica
    for pos in raw_payload:
        # Realiza operação lógica XOR entre o valor acumulado e o byte atual
        crc ^= pos
        # Processa cada um dos 8 bits do byte
        for _ in range(8):
            # Verifica se o bit menos significativo é igual a 1
            if (crc & 0x0001) != 0:
                # Desloca os bits uma posição para a direita
                crc >>= 1
                # Aplica XOR com o polinômio padrão Modbus (0xA001)
                crc ^= 0xA001
            # Caso o bit seja 0
            else:
                # Apenas desloca os bits uma posição para a direita
                crc >>= 1
    # Adiciona o byte LSB do CRC calculado ao final da mensagem Modbus
    raw_payload.append(crc & 0xFF)
    # Adiciona o byte MSB do CRC calculated ao final da mensagem Modbus
    raw_payload.append((crc >> 8) & 0xFF)

    # Adquire a trava mutex garantindo que nenhuma outra requisição escreva ao mesmo tempo no USR
    async with modbus_lock:
        # Bloco de tentativa de transmissão e recepção Modbus
        try:
            # Envia a trama binária completa via socket TCP para o USR-TCP232
            usr_writer.write(raw_payload)
            # Esvazia o buffer e força o envio do pacote binário pela rede
            await usr_writer.drain()

            # Aguarda o retorno da resposta binária do FieldLogger com timeout de 3 segundos
            response = await asyncio.wait_for(usr_reader.read(1024), timeout=3.0)
            # Retorna o objeto JSON contendo o status e os dados em formato hexadecimal para o n8n
            return {
                "status": "success",
                "bytes_hex": response.hex(),
                "raw_bytes": list(response)
            }
        # Captura estouro de tempo no aguardo do retorno do equipamento
        except asyncio.TimeoutError:
            # Lança resposta HTTP 504 (Gateway Timeout) para o n8n
            raise HTTPException(status_code=504, detail="Timeout de resposta do FieldLogger")
        # Captura exceções genéricas de falha na comunicação
        except Exception as e:
            # Lança resposta HTTP 500 informando a mensagem de exceção capturada
            raise HTTPException(status_code=500, detail=str(e))
