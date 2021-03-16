import socket
import sys
from queue import PriorityQueue
import threading
import time
import os
import random

PORTS = {
    '1': 3001,
    '2': 3002,
    '3': 3003,
    '4': 3004,
    '5': 3005
}

state = {
    '1' : True,
    '2' : True,
    '3' : True,
    '4' : True,
    '5' : True
}

processID = sys.argv[1]
server_message = "C" + processID

client_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#connect to servers
try:
    client_socket1.connect((socket.gethostname(), PORTS["1"]))
    client_socket1.send(server_message.encode())
    msg = client_socket1.recv(1024)
    print(msg.decode())
    client_socket1.send(b"ack")
except Exception as e:
    print("cannot connect to client_socket1")
    state['1'] = False

try:
    client_socket2.connect((socket.gethostname(), PORTS["2"]))
    client_socket2.send(server_message.encode())
    msg = client_socket2.recv(1024)
    print(msg.decode())
    client_socket2.send(b"ack")
except Exception as e:
    print("cannot connect to client_socket2")
    state['2'] = False

try:
    client_socket3.connect((socket.gethostname(), PORTS["3"]))
    client_socket3.send(server_message.encode())
    msg = client_socket3.recv(1024)
    print(msg.decode())
    client_socket3.send(b"ack")
except Exception as e:
    print("cannot connect to client_socket3")
    state['3'] = False

try:
    client_socket4.connect((socket.gethostname(), PORTS["4"]))
    client_socket4.send(server_message.encode())
    msg = client_socket4.recv(1024)
    print(msg.decode())
    client_socket4.send(b"ack")
except Exception as e:
    print("cannot connect to client_socket4")
    state['4'] = False

try:
    client_socket5.connect((socket.gethostname(), PORTS["5"]))
    client_socket5.send(server_message.encode())
    msg = client_socket5.recv(1024)
    print(msg.decode())
    client_socket5.send(b"ack")
except Exception as e:
    print("cannot connect to client_socket5")
    state['5'] = False

server_connection = {
    '1' : client_socket1,
    '2' : client_socket2,
    '3' : client_socket3,
    '4' : client_socket4,
    '5' : client_socket5
}



lock = threading.Lock()
received = False
#leader = client_socket1


def timeout1():
    global received
    while True:
        try:
            msg = ""
            msg = client_socket1.recv(1024).decode()
            state[client_socket1] = True
            if (msg != ""):
                received = True
                print("MESSAGE IS:", msg)
        except Exception as e:
            continue

def timeout2():
    global received
    while True:
        try:
            msg = ""
            msg = client_socket2.recv(1024).decode()
            state[client_socket2] = True
            if (msg != ""):
                received = True
                print("MESSAGE IS:", msg)
        except Exception as e:
            continue


def timeout3():
    global received
    while True:
        try:
            msg = ""
            msg = client_socket3.recv(1024).decode()
            state[client_socket3] = True
            if (msg != ""):
                received = True
                print("MESSAGE IS:", msg)
        except Exception as e:
            continue

def timeout4():
    global received
    while True:
        try:
            msg = ""
            msg = client_socket4.recv(1024).decode()
            state[client_socket4] = True
            if (msg != ""):
                received = True
                print("MESSAGE IS:", msg)
        except Exception as e:
            continue

def timeout5():
    global received
    while True:
        try:
            msg = ""
            msg = client_socket5.recv(1024).decode()
            state[client_socket5] = True
            if (msg != ""):
                received = True
                print("MESSAGE IS:", msg)
        except Exception as e:
            continue

threading.Thread(target=timeout1).start()
threading.Thread(target=timeout2).start()
threading.Thread(target=timeout3).start()
threading.Thread(target=timeout4).start()
threading.Thread(target=timeout5).start()

while True:
    command = input()
    received = False
    if(command[10:13] == "get" or command[10:13] == "put"):
        # attach client id to message
        msg = command + "//" + "C" + processID
        #print(leader)
        while True:
            pid = str(random.randint(1,5))
            try:
                time.sleep(2)
                server_connection[pid].send(msg.encode())
                print('Sending message to: Server', pid)
                break
            except Exception as e:
                print('could not send to server', pid)
                continue

        time.sleep(25)
        #print("received: ", str(received))
        if (received == False):
            while True:
                pid = str(random.randint(1,5))
                try:
                    print("Sending leader message to: Server", pid)
                    #server_connection[pid].send(b"leader")
                    # this is the hardcode version (leader is always 2)
                    server_connection[pid].send(b"leader")
                    break
                except Exception as e:
                    continue
    elif (command == "exit"):
        sys.stdout.flush()
        client_socket1.close()
        client_socket2.close()
        client_socket3.close()
        client_socket4.close()
        client_socket5.close()
        os._exit(0)
    else:
        continue



    