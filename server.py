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
#import jsonpickle


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

ballotNum = (0, '0', 0) # ballotnum, processid, depth
acceptNum = (0, 0) # ballotnum, processid, depth
acceptVal = None
leader = None


def exit():
    # pickle dump
    #f = 'outfile' + process_id
    #with open(f, 'wb') as out:
     #   pickle.dump(blockchain, out)

    # a = jsonpickle.encode(blockchain) # create blockchain obj into json
    # with open(f) as json_file:
        
    sys.stdout.flush()
    listen_socket.close()
    server_socket1.close()
    server_socket2.close()
    server_socket3.close()
    server_socket4.close()
    os._exit(0)

# getting messages from everywhere (both clients and servers)
def handle_requests(connection,address):
    while True:
        msg = connection.recv(1024).decode()
        print(msg)
        msg = msg.split(",")

        with lock:
            if (msg == ''):
                exit()
                return
        
            # keep track of client process ids
            elif (msg[0] == 'C1' or msg[0] == 'C2' or msg[0] == 'C3'):
                client_connections[msg[0]] = connection
                m = "server" + process_id + " connected to " + msg[0]
                client_connections[msg[0]].send(m.encode())
                ack = client_connections[msg[0]].recv(1024) 
                print(ack.decode())
            elif (msg[0] == 'leader'):
                Phase1a(msg)
                print('leader')
            elif (msg[0] == 'prepare'):
                Phase1b(msg)
            elif (msg[0] == 'promise'):
                print('promise')
            elif (msg[0] == 'accept'):
                print('promise')
            elif (msg[0] == "Operation(get"):
                # get function

            
        # elif (connection == client_connections['C1']):
        #     print("received message: ", msg.decode(), "from client 1")
        #     connection.send(b"ack")
        #     threading.Thread(target=send_to_all_servers, args=(msg,)).start()
        # else:
        #     print("received message:", msg.decode(), "to server", process_id)

# broadcasting message to servers
def send_to_all_servers(msg):
    #msg = msg + " from server " + process_id
    print("sending " + msg)
    server_socket1.send(msg.encode())
    server_socket2.send(msg.encode())
    server_socket3.send(msg.encode())
    server_socket4.send(msg.encode())

# listen for client initial connection
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
    
def Phase1a():
    ballotNum = (ballotNum[0] + 1, int(process_id), 0)
    msg = "prepare" + "," + str(ballotNum[0]) + "," + str(ballotNum[1]) + "," + str(ballotNum[2]) + "," + process_id
    send_to_all_servers(msg)
    return

# promise
def Phase1b(connection, N):
    if (int(N) > ballotNum[0]):
        msg = "promise".encode()

def Phase2a():
    return

def Phase2b():
    return

def Phase3():
    return
    
if __name__ == "__main__":
    process_id = sys.argv[1]

    #print empty blockchain/BEFORE IMPORT
    #print("------empty blockchain BEFORE import------")
    #print(blockchain)
    
    #f = 'outfile' + process_id
    #if path.exists(f):
        #with open (f, 'rb') as out:
          #  blockchain = pickle.load(out)
    #print blockchain AFTER IMPORT
    # f = 'data' + process_id + '.txt'
    # if path.exists(f):
    #     a = jsonpickle.encode(blockchain)

    #     with open(f) as json_file:
    #         json.load(json_file)
        
        #print("------blockchain AFTER import------")
        #print(blockchain)
    #print blockchain IF blockchain is empty (START)
    #else:
        #dummy1 = operation('get', 'cindy_netid', {'phone_number':'111-222-3333'})
       # dummy2 = operation('get', 'kaylee_netid', {'phone_number':'444-555-6666'})

        #block1 = block(dummy1, 'ABC', 'SHA256')
        #block2 = block(dummy2, 'DEF', 'SHA256')

        #blockchain.append(block1)
        #blockchain.append(block2)


        # print blockchain WITH values
        #print("------blockchain values------")
        #print(blockchain)
        
    # add blockchain to key-value 
    #print("------key value stored------")
    #print(operation._make(dummy1))
    #for i in blockchain:
        #key_value[i.operation.key] = i.operation.value
    
    #print(key_value)

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
        #exit   
        elif (command == "exit"):
            exit()


# 