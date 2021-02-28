import socket
import sys
import threading
import os
import queue
import time


IP = "127.0.0.1"
BUFFER_SIZE = 1024

PORTS = {
    '1': 5000,
    '2': 5001,
    '3': 5002,
    '4': 5003,
    '5': 5004
}

server_connections = {}
client_connections = {}

lock = threading.Lock() 

listen_socket = socket.socket()
listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_socket4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

def exit():
    sys.stdout.flush()
    listen_socket.close()
    server_socket1.close()
    server_socket2.close()
    server_socket3.close()
    server_socket4.close()
    os._exit(0)
    
def handle_requests(connection,address):
    while True:
        msg = connection.recv(1024)
        if (msg == b''):
            exit()
            return
        # # keep track of client process ids
        elif (msg == b'C1' or msg == b'C2' or msg == b'C3'):
            client_connections[msg.decode()] = connection
            m = "server" + process_id + " connected to " + msg.decode()
            client_connections[msg.decode()].send(m.encode())       
        else:
            print("message received: ", msg.decode())


def listen_to_client(listen_socket):
    listen_socket.bind((socket.gethostname(), PORTS[process_id]))
    listen_socket.listen()
    print("listening...")

    while True:
        try:
            connection, address = listen_socket.accept()
            print("connected to " + str(address))
            threading.Thread(target=handle_requests, args=(connection,address)).start()
        except KeyboardInterrupt:
            exit()


if __name__ == "__main__":
    process_id = sys.argv[1]
    #PORT = sys.argv[2]

    #connect to server
    print("Connect to process_id " + process_id)

    threading.Thread(target=listen_to_client, args=(listen_socket,)).start()

    while True:
        command = input()
        #connect to all the other servers
        if (command == "connect"):
            if (process_id == "1"):
                server_socket1.connect((socket.gethostname(), PORTS["2"]))
                server_connections["2"] = server_socket1
                server_socket2.connect((socket.gethostname(), PORTS["3"]))
                server_connections["3"] = server_socket2
                server_socket3.connect((socket.gethostname(), PORTS["4"]))
                server_connections["4"] = server_socket3
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
                server_connections["5"] = server_socket4
            elif (process_id == "2"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_connections["1"] = server_socket1
                server_socket2.connect((socket.gethostname(), PORTS["3"]))
                server_connections["3"] = server_socket2
                server_socket3.connect((socket.gethostname(), PORTS["4"]))
                server_connections["4"] = server_socket3
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
                server_connections["5"] = server_socket4
            elif (process_id == "3"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_connections["1"] = server_socket1
                server_socket2.connect((socket.gethostname(), PORTS["2"]))
                server_connections["2"] = server_socket2
                server_socket3.connect((socket.gethostname(), PORTS["4"]))
                server_connections["4"] = server_socket3
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
                server_connections["5"] = server_socket4
            elif (process_id == "4"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_connections["1"] = server_socket1
                server_socket2.connect((socket.gethostname(), PORTS["2"]))
                server_connections["2"] = server_socket2
                server_socket3.connect((socket.gethostname(), PORTS["3"]))
                server_connections["3"] = server_socket3
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
                server_connections["5"] = server_socket4
            elif (process_id == "5"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_connections["1"] = server_socket1
                server_socket2.connect((socket.gethostname(), PORTS["2"]))
                server_connections["2"] = server_socket2
                server_socket3.connect((socket.gethostname(), PORTS["3"]))
                server_connections["3"] = server_socket3
                server_socket4.connect((socket.gethostname(), PORTS["4"]))
                server_connections["4"] = server_socket4
        # send messages in between servers
        elif (command == "m"):
            command = input("Which server? (1-5 or all): ")
            message = input("What is the message?: ")
            message = message + " -S" + process_id
            if (command == '1'):
                server_connections["1"].send(message.encode())
            elif (command == '2'):
                server_connections["2"].send(message.encode())
            elif (command == '3'):
                server_connections["3"].send(message.encode())
            elif (command == '4'):
                server_connections["4"].send(message.encode())
            elif (command == '5'):
                server_connections["5"].send(message.encode())
            elif (command == 'all'):
                server_socket1.send(message.encode())
                server_socket2.send(message.encode())
                server_socket3.send(message.encode())
                server_socket4.send(message.encode())
        #exit   
        elif (command == "exit"):
            exit()