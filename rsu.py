import sys
import zmq
import time
import argparse

parser = argparse.ArgumentParser(description='Sensor')
parser.add_argument('-port', '--port', type=int, nargs='?', default=5556, help='Port number')
parser.add_argument('-topic', '--topic', nargs='?', default='Temperature', help ='Topic name')
parser.add_argument('-ip', '--ip', nargs='?', default='127.0.0.1', help='IP address to connect to')

args = parser.parse_args()
print(args.ip)
context = zmq.Context()
socket = context.socket(zmq.SUB)


#socket.bind("tcp://*:{}".format(port))

socket.connect("tcp://{}:{}".format(args.ip, args.port))
socket.setsockopt(zmq.SUBSCRIBE, b"")

print('Waiting for message')

while True:
    data = socket.recv()
    data = data.decode()
    topic, message = data.split('-')
    print(topic)
    print(message)
    time.sleep(1)


