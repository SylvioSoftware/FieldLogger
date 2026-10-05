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
    # Registra no log que o equipamento USR-TCP232 estabeleceu a conexão TCP na porta 9001
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
            
            # Adquire a trava mutex para garatir acesso exclusivo à conexão com o USR
            async with modbus_lock:
                # Verifica se a conexão com o hardware USR-TCP232 está ativa
                if usr_writer and usr_reader:
                    # Envia a sequência de bytes diretamente para o socket do USR-TCP232
                    usr_writer.write(data)
                    # Força o esvaziamento da fila do socket enviando todos os dados pela rede
                    await usr_writer.drain()
                    
                    # Bloco de aguardo da resposta com limite de tempo (timeout)
                    try:
                        # Aguarda o retorno de até 4096 bytes do USR com tempo limite de 5 segundos
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
                # Se o USR não estiver conectado no broker
                else:
                    # Registra aviso informando ausência de conexão na porta 9001
                    logger.warning("Notebook enviou dados, mas o USR-TCP232 não está conectado na 9001.")
    # Trata exceções não previstas durante o túnel TCP
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
