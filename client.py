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

#connect to servers
client_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket1.connect((socket.gethostname(), PORTS["1"]))


client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket2.connect((socket.gethostname(), PORTS["2"]))

client_socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket3.connect((socket.gethostname(), PORTS["3"]))

client_socket4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket4.connect((socket.gethostname(), PORTS["4"]))

client_socket5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket5.connect((socket.gethostname(), PORTS["5"]))

while True:
    client_socket1.recv(1024)
