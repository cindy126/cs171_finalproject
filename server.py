
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

# dictionary
PORTS = {
    "1": 3001, 
    "2": 3002, 
    "3": 3003, 
    "4": 3004, 
    "5": 3005
}

# connection states
connection_state = {
    (IP, 3001): True,
    (IP, 3002): True,
    (IP, 3003): True,
    (IP, 3004): True,
    (IP, 3005): True
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

temp_depth = []

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
    if connection_state[addr]:
        s.sendto(msg.encode(),addr)


def send_to_client(socket, msg):
    with lock:
        time.sleep(2)
        print("SENDING TO CLIENT: ", msg)
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
        if (msg == ''):
            print("A client has failed")
            return
    
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
                print('starting leader elections')
            else:
                if (leader == process_id):
                    print("putting operation in queue")
                    q.put(msg)
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
        if (msg == ''):
            print("A server has failed")
            return
        msg = msg.decode().split("&")


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
                print("Received Decide: ", msg)
                insertBlock(msg)
            elif (msg[0] == 'updateP'):
                # updateP,acceptVal, depth
                updateProposerBlockchain(msg)
            elif (msg[0] == 'updateB'):
                #updateBlockchain()
                updateBlockchain(msg)
            elif (msg[0] == 'notifying'):
                leader = msg[1]
                print("My leader is ", leader)
            elif (msg[0] == 'request'):
                updateBlockchain(msg)
            else:
                msg = "&".join(msg)
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
        if (q.queue[0][10:13] == "get" and leader == process_id): #Operation(get,cindy)
            print("Performing get operation")
            with lock:
                # Operation(get, Cindy)//C1
                m = q.queue[0]
                m = m.split(",") # split by comma bc OPERATION
                key = m[1][1:-5]
                client = m[1][-2:]
                
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
            print("ballot num: ", ballotNum)
            if all(p[6] == 'None' for p in promises):
                # all of acceptVal is None
                previousHash = ""
                if len(blockchain) != 0:
                    b = blockchain[-1][0]
                    operation1 = b.op + "," + b.key + "," + b.value
                    # previousHash = operation nonce hash
                    previousHash = operation1 + "||" + blockchain[-1][1] + "||" + blockchain[-1][2]

                previousHash = sha256(previousHash.encode("utf-8")).hexdigest()

                while True:
                    nonce = str(random.randint(0,50))
                    acceptVal = str(q.queue[0]) + "||" + nonce + "||" + previousHash
                    
                    hash = sha256(acceptVal.encode("utf-8")).hexdigest()

                    if "0" <= hash[-1] <= "2":
                        print("hash: ", hash)
                        print("nonce: ", nonce)
                        print("acceptVal: ", acceptVal)
                        break
            else:
                print("----------phase 2a (accept messages sent to servers)---------")
                promises.sort(reverse=True) # descending
                acceptVal = promises[0][-1] # get highest ballot number

            # accept, ballotnum, processid, depth, acceptval
            acceptMessage = 'accept&' + str(ballotNum[0]) + '&' + ballotNum[1] + '&' + str(ballotNum[2]) + '&' + acceptVal
            print("Sending Accept: ", acceptMessage)
            for p in PORTS:
                if p != process_id:
                    threading.Thread(target = send, args = ((IP, PORTS[p]), acceptMessage)).start()
                    time.sleep(0.1)
            #threading.Thread(target = send_to_all_servers, args = (acceptMessage,)).start()

        time.sleep(5) # wait for responses
        # waiting for responses
        
        with lock:
            print("----------phase3 (receiving accept messages)---------")
            print("Total number of accepted: ", str(accepted))
            # did not get the correct number of accepts
            if accepted < 2:
                print("Did not receiv the majority number of accepts")
                q.get()    # check number of accepeted responses
                accepted = 0
                promises = []
                ballotNum = (0, process_id, 0)
                acceptNum = (0, '', 0)
                acceptVal = None 
                continue
        # phase 3 decision
        with lock:
            print("------decision------")
            print("acceptVal: ", acceptVal)
            decideMessage = 'decide&' + acceptVal + '&' + str(ballotNum[2])
            for p in PORTS:
                if p != process_id:
                    threading.Thread(target = send, args = ((IP, PORTS[p]), decideMessage)).start()
                    time.sleep(0.1)
            #threading.Thread(target = send_to_all_servers, args = (decideMessage,)).start()
            
            # parse through operation 
            acceptVal = acceptVal.split('||')
            op = acceptVal[0].split("(")
     
            op1 = op[1].split(",")
            
            # this is Operation
            operation0 = op1[0]
            # this is the key
            key = op1[1][1:]        
            # this is the value
            value = op1[2][1:-5]            
            op = operation(operation0, key, value)
            # get client
            client = op1[2][-2:]
            

            # append block to blockchain
            blockchain.append(block(op, acceptVal[1], acceptVal[2]))
            # add block to key-value 
            key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value

            # send decide
            print("Sending message to client to specific client: ", client)
            threading.Thread(target = send_to_client, args = (client_connections[client], "ack")).start()
            # operation(put, cindy, {dfdsk}adsfdasfasdfasfdsafasfdsfs)

            operate = "Operation(" + operation0 + ", " + key + ", " + value + ")"

            if (operate == q.queue[0][:-4]):
                q.get()
           
            # restart paxos
            accepted = 0
            promises = []
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
        prepareMessage = "prepare&" + str(ballotNum[0]) + "&" + str(ballotNum[1]) + "&" + str(ballotNum[2])
        print("Sending Prepare: ", prepareMessage)
        for p in PORTS:
            if p != process_id:
                threading.Thread(target = send, args = ((IP, PORTS[p]), prepareMessage)).start()
                time.sleep(0.1)
        #threading.Thread(target = send_to_all_servers, args = (prepareMessage,)).start()
    time.sleep(7) # wait for promises
    with lock:
        if len(promises) >= 2:
            leaderMessage = "notifying&" + process_id
            leader = process_id
            for p in PORTS:
                if p != process_id:
                    threading.Thread(target = send, args = ((IP, PORTS[p]), leaderMessage)).start()
                    time.sleep(0.1)
            #threading.Thread(target = send_to_all_servers, args = (leaderMessage,)).start()
            #threading.Thread(target = send_to_all_clients, args = (leaderMessage,)).start()
        else:
            print("NO MAJORITY OF PROMISES")
            print("LEADER ELECTION FAILED")
            return

# get prepare msge from proposer, send out promise
def phase1b(msg):
    print("---------phase1b (promise messages)----------")
    global ballotNum, acceptVal, acceptNum, blockchain
    print("Check Depth")
    # check depth
    currDepth = int(msg[3])
    if(len(blockchain) > currDepth):
        # proposer has shorter depth than blockchain
        for i in range(currDepth, len(blockchain)):
            b = blockchain[i]
            b0 = b[0]
            operation0 = b0.op + "," + b0.key + "," + b0.value
            addBlock = operation0 + '||' + b[1] + '||' + b[2]
            message = "updateP&" + addBlock + "&" + str(i)
            # updateP,acceptVal, depth
            print("Sending updateP Message")
            threading.Thread(target=send, args =((IP, PORTS[msg[2]]), message)).start()
        return
    elif(len(blockchain) < currDepth):
        # proposer has longer depth than blockchain, non-proposers need to update blockchain
        message = "updateB&" + str(len(blockchain)) + "&" + process_id
        print("Sending updateB Message")
        threading.Thread(target=send, args =((IP,PORTS[msg[2]]), message)).start()
        return
    print("Depth passed")

    print("Received Prepare: ", msg)
    # check if received ballot is greater than current ballotNum, update current ballotNum
    receivedBal = (int(msg[1]), msg[2], int(msg[3]))
    if (ballotNum <= receivedBal):
        ballotNum = (int(msg[1]), msg[2], int(msg[3]))
        # promise, ballotNum, processid, depth, acceptNum, processid, depth, acceptVal
        promiseMessage = "promise&" + str(ballotNum[0]) + "&" +  ballotNum[1] + "&" + str(ballotNum[2]) + "&" + str(acceptNum[0]) + "&" + str(acceptNum[1]) + "&" + str(acceptVal)
        print("Sending Promise: ", promiseMessage)
        # send promise to leader
        threading.Thread(target=send, args=((IP, PORTS[msg[2]]), promiseMessage)).start()

        
# receive accept, send out accepted
def phase2b(msg):
    print("---------phase2b (accepted messages)----------")
    global ballotNum, acceptVal, acceptNum, blockchain
    # accept, ballotnum, processid, depth, acceptval
    # ignore messages for shorter blockchains
    currDepth = int(msg[3])
    #print("currDepth: ", currDepth)
    #print("length blockchain: ", len(blockchain))
    if (currDepth > len(blockchain)):
        # proposer has longer depth than blockchain, update blockchain
        message = "updateB&" + str(len(blockchain)) + "&" + process_id
        print("Sending updateB Message")
        threading.Thread(target=send, args=((IP, PORTS[msg[2]]), message)).start()
        return
    elif (currDepth < len(blockchain)):
        return
    print("Received Accept: ", msg)

    # if ballot number is greater, change acceptnum, acceptval
    receivedBal = (int(msg[1]), msg[2], int(msg[3]))
    if(ballotNum <= receivedBal):
        acceptVal = msg[4]
        acceptNum = receivedBal
        acceptedMessage = "accepted&" + str(acceptNum[0]) + "&" + acceptNum[1] + "&" + str(acceptNum[2]) + "&" + str(acceptVal)
        # sending accepted message
        print("Sending Accepted: ", acceptedMessage)
        threading.Thread(target=send, args=((IP, PORTS[msg[2]]), acceptedMessage)).start()

# proposer has a shorter depth
def updateProposerBlockchain(msg):
    global blockchain, acceptVal,temp_depth
    print("UPDATE PROPOSER BLOCK")
    print("proposer block message: ", msg)
    #print("length of msge: ", len(msg))
    # updateP, acceptVal, depth
    # append blocks into proposer
    currDepth = int(msg[2])

    if(currDepth > len(blockchain)):
        temp_depth.append(msg)
        return
    elif (len(blockchain) != currDepth):  # not correct number of blocks yet
        return

    
    # parse through operation 
    # acceptVal = 'put/bill_netid/{“phone_number”: “164-230-9012”}||5||e3b0c44298fc
    acceptV = msg[1].split('||')
    #print("acceptVal: ", acceptV)
    op1 = acceptV[0].split(",")
    #print("op: ", op1)
    operation0 = op1[0]
    key = op1[1]
    value = op1[2]
    op = operation(operation0, key, value)

    # append block to blockchain
    blockchain.append(block(op, acceptV[1], acceptV[2]))
    # add block to key-value 
    key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value

    if (len(temp_depth) == 0):
        return
    else:
        while(len(temp_depth) != 0):
            for n in temp_depth:
                if (int(n[2]) == len(blockchain)):
                    acceptV = n[1].split('||')
                    op1 = acceptV[0].split(",")
                    operation0 = op1[0]
                    key = op1[1]
                    value = op1[2]
                    op = operation(operation0, key, value)
                    # append block to blockchain
                    blockchain.append(block(op, acceptV[1], acceptV[2]))
                    key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value
                    temp_depth.remove(n)
                    break
        return


# proposer has a longer depth
def updateBlockchain(msg):
    global blockchain
    # updateB, depth, process_id
    currDepth = int(msg[1])
    if(len(blockchain) <= currDepth):
        return

    print("UPDATE BLOCKCHAIN")
    # add blocks to the other blocks
    for i in range(currDepth, len(blockchain)):
        b = blockchain[i]
        b0 = b[0]
        operation0 = b0.op + "," + b0.key + "," + b0.value
        addBlock = operation0 + '||' + b[1] + '||' + b[2]
        message = "updateP&" + addBlock + "&" + str(i)
        print("Sending updateP Message")
        # updateP,acceptVal, depth
        threading.Thread(target=send, args =((IP, PORTS[msg[2]]), message)).start()

def insertBlock(msg):
    print("----------inserting block---------")
    global promises, accepted, ballotNum, acceptNum, acceptVal, leader
    #print("LEADER: ", leader)
    print("Received DECIDE from leader, adding block to blockchain")
    # update local acceptVal
    acceptVal = msg[1]
    # parse through operation 
    acceptVal = acceptVal.split('||')
    op = acceptVal[0].split("(")
    op1 = op[1].split(",")
    operation0 = op1[0]
    key = op1[1][1:]
    value = op1[2][1:-5]
    op = operation(operation0, key, value)

    print("this blockchain is being added to chain:")
    print((block(op, acceptVal[1], acceptVal[2])))
    # append block to blockchain
    blockchain.append(block(op, acceptVal[1], acceptVal[2]))
    # add block to key-value 
    key_value[blockchain[-1].operation.key] = blockchain[-1].operation.value

    print("------------added block------------")
    promises = []
    accepted = 0
    ballotNum = (0, process_id, 0)
    acceptNum = (0, '', 0)
    acceptVal = None    
def exit():
    f = 'outfile' + process_id
    with open(f, 'wb') as out:
       pickle.dump(blockchain, out)
        
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
    # thread to listen to servers
    threading.Thread(target=handle_requests_from_server).start()
    # thread to start paxos
    threading.Thread(target=paxos).start()

    # recovery algorithm
    for p in PORTS:
        if p != process_id:
            addr = (IP, PORTS[p])
            msg = 'request&' + str(len(blockchain)) + '&' + process_id
            threading.Thread(target = send, args = (addr, msg)).start()
            time.sleep(0.1)

    while True:
        command = input()
        #connect to all the other servers
        if (command[0:8] == "failLink"): # failLink(P1, P2)
            print("begin failLink")
            connection_state[(IP, PORTS[command[14]])] = False
            print("failing link from P", command[10], "to P", command[14])
        elif (command[0:7] == "fixLink"): # fixLink(P1, P2)
            print("begin fixLink")
            connection_state[(IP, PORTS[command[13]])] = True
            print("fixing link from P", command[9], "to P", command[13])
        elif (command[0:11] == "failProcess"):
            print("begin failProcess")
            f = 'outfile' + process_id
            with open(f, 'wb') as out:
                pickle.dump(blockchain, out)
            sys.stdout.flush()
            listen_socket.close()
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