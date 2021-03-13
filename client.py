import socket
import sys
from queue import PriorityQueue
import threading
import time
import os

PORTS = {
    '1': 5000,
    '2': 5001,
    '3': 5002,
    '4': 5003,
    '5': 5004
}

processID = sys.argv[1]
server_message = "C" + processID

#connect to servers
client_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket1.connect((socket.gethostname(), PORTS["1"]))
client_socket1.send(server_message.encode())
msg = client_socket1.recv(1024)
print(msg.decode())
client_socket1.send(b"ack sent from C1")

client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket2.connect((socket.gethostname(), PORTS["2"]))
client_socket2.send(server_message.encode())
msg = client_socket2.recv(1024)
print(msg.decode())
client_socket2.send(b"ack sent from C1")

client_socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket3.connect((socket.gethostname(), PORTS["3"]))
client_socket3.send(server_message.encode())
msg = client_socket3.recv(1024)
print(msg.decode())
client_socket3.send(b"ack sent from C1")

client_socket4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket4.connect((socket.gethostname(), PORTS["4"]))
client_socket4.send(server_message.encode())
msg = client_socket4.recv(1024)
print(msg.decode())
client_socket4.send(b"ack sent from C1")

client_socket5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket5.connect((socket.gethostname(), PORTS["5"]))
client_socket5.send(server_message.encode())
msg = client_socket5.recv(1024)
print(msg.decode())
client_socket5.send(b"ack sent from C1")

leader = client_socket1


while True:
    command = input()
    if (command[0:9] == "Operation"):
        #Operation(op, key, value)
        #Operation(get, Cindy, 714-122-222)
        leader.send(command.encode())
        if (command[10:13] == "get"):
            print("get")
        elif (command[10:13] == "put"):
            print("put")


    