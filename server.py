import socket
import sys
import threading
import os
import queue
import time
from collections import namedtuple
from hashlib import sha256
import pickle
from os import path
import json


IP = "127.0.0.1"
BUFFER_SIZE = 1024

PORTS = {
    '1': 5000,
    '2': 5001,
    '3': 5002,
    '4': 5003,
    '5': 5004
}

# DATA STRUCTURE INTIALIZATION
# queue to hold temporary operations
q = queue.Queue()

# blockchain intiliazation
# operation: op, key, value
# block: operation, nonce, hash
# blockchain: list
operation = namedtuple('operation',['op', 'key', 'value'])
block = namedtuple('block',['operation', 'nonce', 'hash'])
blockchain = []

# key-value store: dictionary
key_value = {}

#server and client connections
server_connections = {}
client_connections = {}

#client socket (in-socket)
listen_socket = socket.socket()
listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

#server socket (out-socket)
server_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

lock = threading.Lock() 

def exit():
    # pickle dump
    f = 'outfile' + process_id
    with open(f, 'wb') as out:
        pickle.dump(blockchain, out)
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
            ack = client_connections[msg.decode()].recv(1024) 
            print(ack.decode())  
        elif (connection == client_connections['C1']):
            print("received message: ", msg.decode(), "from client 1")
            connection.send(b"ack")
            threading.Thread(target=send_to_all_servers, args=(msg,)).start()
        else:
            print("received message:", msg.decode(), "to server", process_id)

def send_to_all_servers(msg):
    msg = msg.decode()
    msg = msg + " from server " + process_id
    print("sending " + msg)
    server_socket1.send(msg.encode())
    server_socket2.send(msg.encode())
    server_socket3.send(msg.encode())
    server_socket4.send(msg.encode())

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

    #print empty blockchain/BEFORE IMPORT
    print("------empty blockchain BEFORE import------")
    print(blockchain)
    
    # f = 'outfile' + process_id
    # if path.exists(f):
    #     with open (f, 'rb') as out:
    #         blockchain = pickle.load(out)

    with open('data.json', 'w') as f:
        json.dump(blockchain, f)

    #print blockchain AFTER IMPORT
    print("------blockchain AFTER import------")
    print(blockchain)

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
        elif (command == 'b'):
            dummy1 = operation('get', 'cindy_netid', {'phone_number':'111-222-3333'})
            dummy2 = operation('get', 'kaylee_netid', {'phone_number':'444-555-6666'})

            block1 = block(dummy1, 'ABC', 'SHA256')
            block2 = block(dummy2, 'DEF', 'SHA256')

            blockchain.append(block1)
            blockchain.append(block2)

            # print blockchain WITH values
            print("------blockchain with values------")
            print(blockchain)
        #exit   
        elif (command == "exit"):
            exit()