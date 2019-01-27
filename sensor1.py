import zmq
import csv
import sys
import time
import argparse

parser = argparse.ArgumentParser(description='Sensor')
parser.add_argument('-port', '--port', type=int, nargs='?', default=5556, help='Port number')
parser.add_argument('-topic', '--topic', nargs='?', default='Temperature', help ='Topic name')
parser.add_argument('-ip', '--ip', nargs='?', default='10.0.0.1', help='IP address to connect to')

args = parser.parse_args()
print(args.port)
print(args.topic)
print(args.ip)

def csvread(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            msgdata.append(row[0])


msgdata = []
csvread("Temperature.csv")

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://{}:{}".format(args.ip, args.port))


while True:
    for items in msgdata:
        msg = "{}-{}".format(args.topic, items)
        msg = msg.encode('utf-8')
        print(msg)
        socket.send(msg)
        time.sleep(1)



