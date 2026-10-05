import socket
import select
import time

# Configurações do USR-TCP232
USR_IP = "192.168.1.200"   # Altere para o IP real do seu USR na rede local
USR_PORT = 9001           # Porta atual do USR (não precisa alterar no local)

# Portas dos Clientes no Broker Docker
PORT_NOTEBOOK = 9002      # Porta Exclusiva/Prioritária (FieldLogger Config)
PORT_N8N = 5000           # Porta de Rotina (n8n)

FL_TIMEOUT_PRIORITY = 3.0  # Tempo em segundos de prioridade após a última transmissão do Notebook

def main():
    # Socket para comunicar com o USR-TCP232
    usr_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    usr_sock.connect((USR_IP, USR_PORT))
    usr_sock.setblocking(False)

    # Socket Servidor para o Notebook (Porta 9002)
    server_fl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_fl.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_fl.bind(('0.0.0.0', PORT_NOTEBOOK))
    server_fl.listen(5)

    # Socket Servidor para o n8n (Porta 5000)
    server_n8n = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_n8n.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_n8n.bind(('0.0.0.0', PORT_N8N))
    server_n8n.listen(5)

    clients_fl = []
    clients_n8n = []

    last_fl_activity = 0

    print(f"Broker rodando: Notebook [Porta {PORT_NOTEBOOK}] | n8n [Porta {PORT_N8N}] -> USR-TCP232 [{USR_IP}:{USR_PORT}]")

    while True:
        readable = [server_fl, server_n8n, usr_sock] + clients_fl + clients_n8n
        r_list, _, _ = select.select(readable, [], [], 0.1)

        now = time.time()
        in_priority_mode = (now - last_fl_activity) < FL_TIMEOUT_PRIORITY

        for s in r_list:
            # 1. Nova conexão do Notebook
            if s == server_fl:
                conn, addr = server_fl.accept()
                clients_fl.append(conn)
                print(f"[NOTEBOOK] Conectado de {addr}")

            # 2. Nova conexão do n8n
            elif s == server_n8n:
                conn, addr = server_n8n.accept()
                clients_n8n.append(conn)
                print(f"[N8N] Conectado de {addr}")

            # 3. Pacotes vindos do Notebook (PRIORIDADE)
            elif s in clients_fl:
                data = s.recv(1024)
                if data:
                    last_fl_activity = time.time()  # Atualiza a trava de prioridade
                    usr_sock.sendall(data)          # Encaminha direto para o USR
                else:
                    clients_fl.remove(s)
                    s.close()

            # 4. Pacotes vindos do n8n (BAIXA PRIORIDADE)
            elif s in clients_n8n:
                data = s.recv(1024)
                if data:
                    if in_priority_mode:
                        # Se o Notebook estiver baixando dados, descarta o envio do n8n
                        print("[BLOQUEIO] Requisição do n8n ignorada (Coleta do Notebook ativa)")
                    else:
                        usr_sock.sendall(data)
                else:
                    clients_n8n.remove(s)
                    s.close()

            # 5. Resposta vinda do FieldLogger (via USR-TCP232)
            elif s == usr_sock:
                data = usr_sock.recv(1024)
                if data:
                    if in_priority_mode and clients_fl:
                        for client in clients_fl:
                            client.sendall(data)
                    elif clients_n8n:
                        for client in clients_n8n:
                            client.sendall(data)

if __name__ == "__main__":
    main()
