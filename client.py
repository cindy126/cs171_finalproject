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

if __name__ == "__main__":
    processID = sys.argv[1]

    while True:
        command = input("Which server? (1-5 or all): ")
        msg = input("What is the message?: ")
        msg = msg + " -C" + processID
        if command == '1':
            client_socket1.send(msg.encode())
        elif command == '2':
            client_socket2.send(msg.encode())
        elif command == '3':
            client_socket3.send(msg.encode())
        elif command == '4':
            client_socket4.send(msg.encode())
        elif command == '5':
            client_socket5.send(msg.encode())
        elif command == 'all':
            client_socket1.send(msg.encode())
            client_socket2.send(msg.encode())
            client_socket3.send(msg.encode())
            client_socket4.send(msg.encode())
            client_socket5.send(msg.encode())