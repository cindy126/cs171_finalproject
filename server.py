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
            #msg = connection.recv(1024)
            #print("message received: ", msg.decode())
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
        #connect
        if (command == "connect"):
            if (process_id == "1"):
                server_socket1.connect((socket.gethostname(), PORTS["2"]))
                server_socket2.connect((socket.gethostname(), PORTS["3"]))
                server_socket3.connect((socket.gethostname(), PORTS["4"]))
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
            elif (process_id == "2"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_socket2.connect((socket.gethostname(), PORTS["3"]))
                server_socket3.connect((socket.gethostname(), PORTS["4"]))
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
            elif (process_id == "3"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_socket2.connect((socket.gethostname(), PORTS["2"]))
                server_socket3.connect((socket.gethostname(), PORTS["4"]))
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
            elif (process_id == "4"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_socket2.connect((socket.gethostname(), PORTS["2"]))
                server_socket3.connect((socket.gethostname(), PORTS["3"]))
                server_socket4.connect((socket.gethostname(), PORTS["5"]))
            elif (process_id == "5"):
                server_socket1.connect((socket.gethostname(), PORTS["1"]))
                server_socket2.connect((socket.gethostname(), PORTS["2"]))
                server_socket3.connect((socket.gethostname(), PORTS["3"]))
                server_socket4.connect((socket.gethostname(), PORTS["4"]))
        elif (command == "send message"):
            input("Which server?")
            intput("What is the message?: ")
        #exit   
        elif (command == "exit"):
            exit()