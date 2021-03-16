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
import random
#import jsonpickle


IP = "127.0.0.1"
BUFFER_SIZE = 1024

#receive process_id
process_id = sys.argv[1]

# Dictionary containing all ports of all processes
PORTS = {
    "1": 3001, 
    "2": 3002, 
    "3": 3003, 
    "4": 3004, 
    "5": 3005
}

# Dictionary containing the state of each connection
# Address: Connection state (bool)
CONN_STATE = {
    (IP, 3001): True,
    (IP, 3002): True,
    (IP, 3003): True,
    (IP, 3004): True,
    (IP, 3005): True
}

# For testing purposes
addr_to_PID = {
    (IP, 3001): '1',
    (IP, 3002): '2',
    (IP, 3003): '3',
    (IP, 3004): '4',
    (IP, 3005): '5'
}

# DATA STRUCTURE INTIALIZATION
# queue to hold temporary operations
q = queue.Queue()

# blockchain intiliazation
# operation: op, key, value
# block: operation, nonce, hash
# blockchain: list

#block ("operation(get, cindy, {71})", nonce, hash)
operation = namedtuple('operation',['op', 'key', 'value'])
block = namedtuple('block',['operation', 'nonce', 'hash'])
blockchain = []

# key-value store: dictionary
key_value = {}

# ballotnum, processid, depth
ballotNum = (0, '0', 0) 

# ballotnum,
acceptNum = (0, '', 0) # ballotnum, processid, depth

# acceptVal
acceptVal = None

leader = None

# list of servers that return a promise to the leader
promises = []


# accepted messages response
accepted = 0

# client connections
client_connections = {}

# client socket (in-socket)
listen_socket = socket.socket()
listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Create server socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((IP, PORTS[process_id]))

lock = threading.Lock() 

# listen for client initial connection
def listen_to_client(listen_socket):
    listen_socket.bind((socket.gethostname(), PORTS[process_id]))
    listen_socket.listen()
    print("listening...")

    while True:
        try:
            connection, address = listen_socket.accept()
            print("connected to " + str(address))
            threading.Thread(target=handle_requests_from_client, args=(connection,address)).start()
        except KeyboardInterrupt:
            exit()

def send(addr, msg):
    # Fixed delay for each message
    time.sleep(2)
    print("sending.....")
    # Check if connection is valid
    if CONN_STATE[addr]:
        s.sendto(msg.encode(),addr)


def send_to_client(socket, msg):
    with lock:
        time.sleep(2)
        print("SENDING TO CLIENT ", msg)
        socket.send(msg.encode())

# broadcast to all clients
def send_to_all_clients(msg):
    print("Sending to all clients: ", msg)
    client_connections["C1"].send(msg.encode())
    client_connections["C2"].send(msg.encode())
    client_connections["C3"].send(msg.encode())

def handle_requests_from_client(connection,address):
    while True:
        msg = connection.recv(1024).decode()
        #print("THIS IS THE THE MESSAGE: ", msg)
        if (msg == ''):
            print("another server has failed")
            return
        print(msg)
    
        with lock:
            # keep track of client process ids
            if (msg == 'C1' or msg == 'C2' or msg == 'C3'):    # client connection message
                client_connections[msg] = connection
                m = "server" + process_id + " connected to " + msg
                client_connections[msg].send(m.encode())
                ack = client_connections[msg].recv(1024)
                print(ack.decode())
            elif (msg == 'leader'):
                threading.Thread(target=leader_election).start() # start leader election
                #print('leader')
            else:
                if (leader == process_id):
                    print("putting operation in queue")
                    q.put(msg)
                    for i in range(q.qsize()):
                        print(q.queue[i])

                elif (leader != None):
                    threading.Thread(target = send, args = ((IP, PORTS[leader]), msg)).start()
                    print("forwarding message to leader")
                else:
                    continue

# getting messages from everywhere (both clients and servers)
def handle_requests_from_server():
    global leader, accepted
    while True:
        msg, addr = s.recvfrom(1024)
        print("THIS IS THE THE MESSAGE: ", msg)
        if (msg == ''):
            print("OH NO")
            return
        msg = msg.decode().split(",")
    
        with lock:
            if (msg[0] == 'prepare'): # leader send prepare to all other servers
                # pepare, ballonum, processid, depth
                phase1b(msg)    # how other servers will respond to the prepare
            elif (msg[0] == 'promise'):
                # promise, ballotNum, processid, depth, acceptNum, processid, depth, acceptVal
                print("Received Promise: ", msg)
                # msg = list
                # promises = list of promise
                promises.append(msg)
            elif (msg[0] == 'accept'):
                # accept, ballotnum, processid, depth, acceptval
                phase2b(msg)
            elif (msg[0] == 'accepted'):
                length = int(msg[3])
                if (len(blockchain) != length):
                    continue
                accepted += 1
                print("Received Accepted: ", msg)
            elif (msg[0] == 'decide'):
                print("Received decide: ", msg)
                insertBlock(msg)
            elif (msg[0] == 'updateP'):
                # updateP,acceptVal, depth
                updateProposerBlockchain(msg)
            elif (msg[0] == 'updateB'):
                print('promise')
                #updateBlockchain()
                updateBlockchain(msg)
            elif (msg[0] == 'notifying'):
                leader = msg[1]
                print("My leader is ", leader)
            else:
                msg = ",".join(msg)
                print(msg)
                if (leader == process_id):
                    print("putting operation in queue")
                    q.put(msg)
                    for i in range(q.qsize()):
                        print(q.queue[i])
                elif (leader != None):
                    threading.Thread(target = send, args = ((IP, PORTS[leader]), msg)).start()
                    print("forwarding message to leader")
                else:
                    continue

def paxos():
    global ballotNum, promises, blockchain, promises, acceptVal, accepted, key_value
    while True:
        if (q.empty()):
            continue 
        # queue is NOT empty
        if (q.queue[0][10:13] == "get"): #Operation(get,cindy)
            print("GETGETGET")
            with lock:
                print('hello') #Operation(get, Cindy)//C1
                m = q.queue[0]
                m = m.split(",")
                key = m[1][1:-5]
                client = m[1][-2:]
                print(client)
                
                if key in key_value:
                    get_message = key_value[key]
                else:
                    get_message = "NO_KEY"
                threading.Thread(target = send_to_client, args = (client_connections[client], get_message)).start()
                q.get()
                continue
        with lock:
            print("--------In Paxos-------")
            # HERE"S WHERE YOU DO THE BLOCK
            # set myVal
            ballotNum = (ballotNum[0], process_id, len(blockchain))
            if all(p[6] == 'None' for p in promises):
                # all of acceptVal is None
                previousHash = ""
                if len(blockchain) != 0:
                    print("ALISDJF: ", blockchain[-1][0])
                    print(type(blockchain[-1][0]))
                    b = blockchain[-1][0]
                    operation1 = b.op + b.key + b.value
                    print(operation1)
                    # previousHash = operation nonce hash
                    previousHash = operation1 + "||" + blockchain[-1][1] + "||" + blockchain[-1][2]
                previousHash = sha256(previousHash.encode("utf-8")).hexdigest()

                while True:
                    nonce = str(random.randint(0,50))
                    acceptVal = str(q.queue[0]) + "||" + nonce + "||" + previousHash
                    hash = sha256(acceptVal.encode('utf-8')).hexdigest()

                    if "0" <= hash[-1] <= "2":
                        print("Nonce: ", nonce)
                        print("Hash value: ", hash)
                        print("acceptVal: ", acceptVal)
                        break
            else:
                promises.sort(reverse=True) # descending
                acceptVal = promises[0][-1] # get highest ballot number
            # accept, ballotnum, processid, depth, acceptval
            acceptMessage = 'accept,' + str(ballotNum[0]) + ',' + ballotNum[1] + ',' + str(ballotNum[2]) + ',' + acceptVal
            print("Sending Accept: ", acceptMessage)
            for conn in PORTS:
                if conn != process_id:
                    threading.Thread(target = send, args = ((IP, PORTS[conn]), acceptMessage)).start()
                    time.sleep(0.1)
            #threading.Thread(target = send_to_all_servers, args = (acceptMessage,)).start()

        time.sleep(4.5) # wait for responses
        
        with lock:
            print("total number of accepted: ", str(accepted))
            if accepted < 2:    # check number of accepeted responses
                continue
        # phase 3 decision
        with lock:
            print("------decision------")
            print("Sending Decide Message")
            decideMessage = 'decide,' + acceptVal + ',' + str(ballotNum[2])
            for conn in PORTS:
                if conn != process_id:
                    threading.Thread(target = send, args = ((IP, PORTS[conn]), decideMessage)).start()
                    time.sleep(0.1)
            #threading.Thread(target = send_to_all_servers, args = (decideMessage,)).start()
            
            # parse through operation 
            acceptVal = acceptVal.split('||')
            print("acceptVal ", acceptVal)
            op = acceptVal[0].split("(")
            print(op)
            op1 = op[1].split(",")
            print(op1)
            # this is Operation
            operation0 = op1[0]
            print(operation0)
            # this is the key
            key = op1[1][1:]
            print(key)
            # this is the value
            value = op1[2][1:-5]
            print(value)
            op = operation(operation0, key, value)
            # get client
            client = op1[2][-2:]
            print(client)

            # append block to blockchain
            blockchain.append(block(op, acceptVal[1], acceptVal[2]))
            # add block to key-value 
            key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value

            # send decide
            print("Sending message to client to specific client")
            print("Client is: ", client)
            threading.Thread(target = send_to_client, args = (client_connections[client], "ack")).start()

    
            #print(key_value)
            
            print(blockchain)
            q.get()
           
            # restart paxos
            promises = []
            accepted = 0
            ballotNum = (0, process_id, 0)
            acceptNum = (0, '', 0)
            acceptVal = None      


def leader_election():
    global ballotNum, promises, blockchain, promises, leader
    print("--- In Leader Election ---")
    with lock:
        promises = []
        ballotNum = (ballotNum[0] + 1, process_id, len(blockchain))
        # pepare, ballonum, processid, depth
        prepareMessage = "prepare" + "," + str(ballotNum[0]) + "," + str(ballotNum[1]) + "," + str(ballotNum[2])
        print("Sending Prepare: ", prepareMessage)
        for conn in PORTS:
            if conn != process_id:
                threading.Thread(target = send, args = ((IP, PORTS[conn]), prepareMessage)).start()
                time.sleep(0.1)
        #threading.Thread(target = send_to_all_servers, args = (prepareMessage,)).start()
    time.sleep(4.5) # wait for promises
    with lock:
        if len(promises) >= 2:
            leaderMessage = "notifying," + process_id
            leader = process_id
            for conn in PORTS:
                if conn != process_id:
                    threading.Thread(target = send, args = ((IP, PORTS[conn]), leaderMessage)).start()
                    time.sleep(0.1)
            #threading.Thread(target = send_to_all_servers, args = (leaderMessage,)).start()
            #threading.Thread(target = send_to_all_clients, args = (leaderMessage,)).start()
        else:
            print("no majority of promises")
            return

# get prepare msge from proposer, send out promise
def phase1b(msg):
    global ballotNum, acceptVal, acceptNum, blockchain
    print("Check Depth")
    # check depth
    print(msg)
    currDepth = int(msg[3])
    if(len(blockchain) > currDepth):
        # proposer has shorter depth than blockchain
        for i in range(currDepth, len(blockchain)):
            b = blockchain[i]
            print(b)
            addBlock = b[0] + '||' + b[1] + '||' + b[2]
            message = "updateP," + addBlock + "," + str(i)
            # updateP,acceptVal, depth
            print("Sending UpdateP Message")
            threading.Thread(target=send, args =((IP, PORTS[[msg[2]]]), message)).start()
        return
    elif(len(blockchain) < currDepth):
        # proposer has longer depth than blockchain, need to update blockchain
        message = "updateB," + str(len(blockchain))
        threading.Thread(target=send, args =((IP,PORTS[msg[2]]), message)).start()
        return
    print("Depth passed")

    print("Received Prepare: ", msg)
    # check if received ballot is greater than current ballotNum, update current ballotNum
    receivedBal = (int(msg[1]), msg[2], int(msg[3]))
    if (ballotNum <= receivedBal):
        ballotNum = (int(msg[1]), msg[2], int(msg[3]))
        # promise, ballotNum, processid, depth, acceptNum, processid, depth, acceptVal
        promiseMessage = "promise" + "," + str(ballotNum[0]) + "," +  ballotNum[1] + "," + str(ballotNum[2]) + "," + str(acceptNum[0]) + "," + str(acceptNum[1]) + "," + str(acceptVal)
        print("Sending Promise: ", promiseMessage)
        # send promise to leader
        threading.Thread(target=send, args=((IP, PORTS[msg[2]]), promiseMessage)).start()

        
# receive accept, send out accepted
def phase2b(msg):
    print("I AM IN PHASE2B")
    global ballotNum, acceptVal, acceptNum, blockchain
    # accept, ballotnum, processid, depth, acceptval
    # ignore messages for shorter blockchains
    currDepth = int(msg[3])
    print("currDepth: ", currDepth)
    print("length blockchain: ", len(blockchain))
    if (currDepth > len(blockchain)):
        # proposer has longer depth than blockchain, update blockchain
        message = "updateB," + str(len(blockchain))
        threading.Thread(target=send, args=((IP, PORTS[msg[2]]), message)).start()
        return
    elif (currDepth < len(blockchain)):
        print("IM NOT DOING SHIT")
        return
    print("Received Accept: ", msg)

    # if ballot number is greater, change acceptnum, acceptval
    receivedBal = (int(msg[1]), msg[2], int(msg[3]))
    if(ballotNum <= receivedBal):
        acceptVal = msg[4]
        acceptNum = receivedBal
        acceptedMessage = "accepted," + str(acceptNum[0]) + "," + acceptNum[1] + "," + str(acceptNum[2]) + "," + str(acceptVal)
        # sending accepted message
        print("Sending Accepted: ", acceptedMessage)
        threading.Thread(target=send, args=((IP, PORTS[msg[2]]), acceptedMessage)).start()


def updateProposerBlockchain(msg):
    global blockchain, acceptVal
    print("UPDATE PROPOSER BLOCK")
    # updateP, acceptVal, depth
    # append blocks into proposer
    currDepth = int(msg[2])
    if (len(blockchain) != currDepth):  # not correct number of blocks yet
        return
    
    # parse through operation 
    acceptVal = acceptVal.split('||')
    op = acceptVal[0].split("(")
    op1 = op[1].split(",")
    operation0 = op1[0]
    key = op1[1][1:]
    value = op1[2][1:-5]
    op = operation(operation0, key, value)

    # append block to blockchain
    blockchain.append(block(op, acceptVal[1], acceptVal[2]))
    # add block to key-value 
    key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value
    
def updateBlockchain(msg):
    global blockchain
    print("UPDATE BLOCKCHAIN")
    # updateB, depth
    currDepth = int(msg[1])
    print("update:, ",msg)
    if(len(blockchain) <= currDepth):
        return

    # add blocks to proposer
    for i in range(currDepth, len(blockchain)):
        b = blockchain[i]
        addBlock = b[0] + '||' + b[1] + '||' + b[2]
        message = "updateP," + addBlock + "," + str(i)
        # updateP,acceptVal, depth
        threading.Thread(target=send, args =((IP, PORTS[msg[leader]]), message)).start()

def insertBlock(msg):
    global promises, accepted, ballotNum, acceptNum, acceptVal
    print("Received DECIDE from P" + leader + ", adding block to blockchain")
    # update local acceptVal

    print(msg)
    acceptVal = msg[1] + "," + msg[2] + "," + msg[3]
    print(acceptVal)
    
    # parse through operation 
    acceptVal = acceptVal.split('||')
    op = acceptVal[0].split("(")
    op1 = op[1].split(",")
    operation0 = op1[0]
    key = op1[1][1:]
    value = op1[2][1:-5]
    op = operation(operation0, key, value)

    print(op)
    print(acceptVal)
    
    # append block to blockchain
    blockchain.append(block(op, acceptVal[1], acceptVal[2]))
    # add block to key-value 
    key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value

    promises = []
    accepted = 0
    ballotNum = (0, process_id, 0)
    acceptNum = (0, '', 0)
    acceptVal = 'NULL'
    
def exit():
    # f = 'outfile' + process_id
    # with open(f, 'wb') as out:
    #    pickle.dump(blockchain, out)

    # a = jsonpickle.encode(blockchain) # create blockchain obj into json
    # with open(f) as json_file:
        
    sys.stdout.flush()
    listen_socket.close()
    os._exit(0)


if __name__ == "__main__":
    print("------empty blockchain BEFORE import------")
    print(blockchain)
    
    f = 'outfile' + process_id
    if path.exists(f):
        with open (f, 'rb') as out:
           blockchain = pickle.load(out)

    print("------blockchain with values------")
    print(blockchain)
        
    print("------key value stored------")
    for i in blockchain:
        key_value[i.operation.key] = i.operation.value
    print(key_value)

    #connect to server
    print("Connect to process_id " + process_id)

    # thread to listen to clients
    threading.Thread(target=listen_to_client, args=(listen_socket,)).start()
    threading.Thread(target=handle_requests_from_server).start()
    # thread to start paxos
    threading.Thread(target=paxos).start()

    while True:
        command = input()
        #connect to all the other servers
        if (command[0:8] == "failLink"): # failLink(P1, P2)
            print("begin failLink")
            CONN_STATE[(IP, PORTS[command[14]])] = False
        elif (command[0:7] == "fixLink"): # fixLink(P1, P2)
            print("begin fixLink")
            CONN_STATE[(IP, PORTS[command[13]])] = True
        elif (command[0:11] == "failProcess"):
            print("begin failProcess")
            print(blockchain)
            print(type(blockchain))
            f = 'outfile' + process_id
            with open(f, 'wb') as out:
                pickle.dump(blockchain, out)
            os._exit(0)
        elif (command == "printBlockchain"):
            if (len(blockchain) == 0):
                print("Blockchain is empty")
            for i in range(len(blockchain)):
                bl = blockchain[i]
                print("---------- block " + str(i+1) + "----------")
                print('operations:', bl[0])
                print('nonce:', bl[1])
                print('hash:', bl[2])
                print('------- end blockchain -------')
        elif (command == "printKVStore"):
            print("-------- key values -----------")
            print(key_value)
        elif (command == "printQueue"):
            for i in range(q.qsize()):
                print(q.queue[i])
        elif (command == "exit"):
            exit()