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

PORTS = {
    '1': 5000,
    '2': 5001,
    '3': 5002,
    '4': 5003,
    '5': 5004
}

states = {
    (IP, 5000): True,
    (IP, 5001): True,
    (IP, 5002): True,
    (IP, 5003): True,
    (IP, 5004): True
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

#server and client connections
server_connections = {} #1, #2, #3. #4. #5
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

def send(socket, msg):
    time.sleep(2)
    print("SEND: ", msg)
    socket.send(msg.encode())

# broadcasting message to servers
def send_to_all_servers(msg):
    #msg = msg + " from server " + process_id
    print("Sending to all servers: " + msg)
    server_socket1.send(msg.encode())
    server_socket2.send(msg.encode())
    server_socket3.send(msg.encode())
    server_socket4.send(msg.encode())

# broadcast to all clients
def send_to_all_clients(msg):
    print("Sending to all clients: ", msg)
    client_connections["C1"].send(msg.encode())
    # client_connections["C2"].send(msg.encode())
    # client_connections["C3"].send(msg.encode())

# getting messages from everywhere (both clients and servers)
def handle_requests(connection,address):
    global leader, accepted
    while True:
        msg = connection.recv(BUFFER_SIZE).decode()
        #print("THIS IS THE THE MESSAGE: ", msg)
        if (msg == ''):
            exit()
            return
        msg = msg.split(",")
    
        with lock:
            # keep track of client process ids
            if (msg[0] == 'C1' or msg[0] == 'C2' or msg[0] == 'C3'):    # client connection message
                client_connections[msg[0]] = connection
                m = "server" + process_id + " connected to " + msg[0]
                client_connections[msg[0]].send(m.encode())
                ack = client_connections[msg[0]].recv(BUFFER_SIZE) 
                print(ack.decode())
            elif (msg[0] == 'leader'):
                threading.Thread(target=leader_election).start() # start leader election
                #print('leader')
            elif (msg[0] == 'prepare'): # leader send prepare to all other servers
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
                print("promise")
            elif (msg[0] == 'updateB'):
                print('promise')
                #updateBlockchain()
            elif (msg[0] == 'notifying'):
                leader = msg[1]
                print("My leader is ", leader)
            else:
                msg = ",".join(msg)
                print(msg)
                if (leader == process_id):
                    print("putting operation in queue")
                    q.put(msg)
                    if (q.empty()):
                        print("TRUE")
                    else:
                        print("FALSE")

                elif (leader != None):
                    server_connections[leader].send(msg.encode())
                    print("forwarding message to leader")
                else:
                    continue
                
        # elif (connection == client_connections['C1']):
        #     print("received message: ", msg.decode(), "from client 1")
        #     connection.send(b"ack")
        #     threading.Thread(target=send_to_all_servers, args=(msg,)).start()
        # else:
        #     print("received message:", msg.decode(), "to server", process_id)
def paxos():
    global ballotNum, promises, blockchain, promises, acceptVal, accepted, key_value
    while True:
        if (q.empty()):
            continue 
        # queue is NOT empty
        if (q.queue[0][10:13] == "get"): #Operation(get,cindy)
            print("GETGETGET")
            with lock:
                print('hello')
                m = q.queue[0]
                m = m.split(",")
                key = m[1][:-1]
                if key in key_value:
                    print(key_value[key])
                else:
                    print("NO_KEY")
                q.get()
                continue

        with lock:
            print("--------In Paxos-------")
            # HERE"S WHERE YOU DO THE BLOCK
            # set myVal
            if all(p[6] == 'None' for p in promises):
                # all of acceptVal is None
                previousHash = ""
                if len(blockchain) != 0:
                    previousHash = blockchain[-1][0] + "||" + blockchain[-1][1] + "||" + blockchain[-1][2]
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
            threading.Thread(target = send_to_all_servers, args = (acceptMessage,)).start()

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
            threading.Thread(target = send_to_all_servers, args = (decideMessage,)).start()
            
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
            threading.Thread(target = send, args = (client_connections[client], "ack")).start()

    
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
        threading.Thread(target = send_to_all_servers, args = (prepareMessage,)).start()
    time.sleep(4.5) # wait for promises
    with lock:
        if len(promises) >= 2:
            leaderMessage = "notifying," + process_id
            leader = process_id
            threading.Thread(target = send_to_all_servers, args = (leaderMessage,)).start()
            #threading.Thread(target = send_to_all_clients, args = (leaderMessage,)).start()
        else:
            print("no majority of promises")
            return

# get prepare msge from proposer, send out promise
def phase1b(msg):
    global ballotNum, acceptVal, acceptNum, blockchain
    print("Check Depth")
    # check depth
    currDepth = int(msg[3])
    if(len(blockchain) > currDepth):
        # proposer has shorter depth than blockchain
        for i in range(currDepth, len(blockchain)):
            b = blockchain[i]
            addBlock = b[0] + '||' + b[1] + '||' + b[2]
            message = "updateP," + addBlock + "," + str(i)
            # updateP,acceptVal, depth
            threading.Thread(target=send, args =(server_connections[leader], message)).start()
        return
    elif(len(blockchain) < currDepth):
        # proposer has longer depth than blockchain
        message = "updateB," + str(len(blockchain))
        threading.Thread(target=send, args =(server_connections[leader], message)).start()
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
        threading.Thread(target=send, args=(server_connections[msg[2]], promiseMessage)).start()

        
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
        threading.Thread(target=send, args=(server_connections[msg[2]], message)).start()
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
        threading.Thread(target=send, args=(server_connections[msg[2]], acceptedMessage)).start()


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
    if(len(blockchain) <= currDepth):
        return

    # add blocks to proposer
    for i in range(currDepth, len(blockchain)):
        b = blockchain[i]
        addBlock = b[0] + '||' + b[1] + '||' + b[2]
        message = "updateP," + addBlock + "," + str(i)
        # updateP,acceptVal, depth
        threading.Thread(target=send, args =(server_connections[leader], message)).start()

def insertBlock(msg):
    global promises, accepted, ballotNum, acceptNum, acceptVal
    print("Received DECIDE from P" + leader + ", adding block to blockchain")
    # update local acceptVal

    acceptVal = msg[1] + "," + msg[2] + "," + msg[3]
    
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

    promises = []
    accepted = 0
    ballotNum = (0, process_id, 0)
    acceptNum = (0, '', 0)
    acceptVal = 'NULL'
    
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


if __name__ == "__main__":

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
    threading.Thread(target=paxos).start()

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

        # elif (command[0:8] == "failLink"):  # failLink(P1, P2)
        #     states[(IP, PORTS[command[14]])] = False
        #     print("fail link")
        # elif (command[0:7] == "fixLink"):
        #     states[(IP, PORTS[command(13)])] = True
        #     print("fixLink")
        # elif (command[0:7] == "fixLink"): # fixLink(P1, P2)
        #     states[(IP, PORTS[command(13)])] = True
        #     print("fixLink")
        # elif (command[0:11] == "failProcess"):
        #     print("failProcess")
        elif (command == "printBlockchain"):
            for i in range(len(blockchain)):
                block = blockchain[i]
                print("---------- block " + str(i+1) + "----------")
                print('operations:', block[0])
                print('nonce:', block[1])
                print('hash:', block[2])
                print('----------- end blockchain ------------')
        elif (command == "printKVStore"):
            print("-------- key values -----------")
            print(key_value)
        elif (command == "printQueue"):
            for(i in range())
        elif (command == "exit"):
            exit()