import zmq
from threading import Thread
import time

context = zmq.Context()
pub_socket = context.socket(zmq.PUB)
sub_socket = context.socket(zmq.SUB)
req_socket = context.socket(zmq.REQ)
rep_socket = context.socket(zmq.REP)


def register_pub(topic, pubID):
    req_socket.connect("tcp://127.0.0.1:5512")  # We can do a configuration file for the IP and ports
    req_socket.send_string("{}-{}".format(topic, pubID))
    print(req_socket.recv_string())


def register_sub(topic, subID):
    req_socket.connect("tcp://127.0.0.1:5513")  # We can do a configuration file for the IP and ports
    req_socket.send_string("{}-{}".format(topic, subID))


def publish(topic, value):
    pub_socket.connect("tcp://127.0.0.1:5510")
    pub_socket.send_string("{}-{}".format(topic, value))


def notify(topic, value):
    sub_socket.recv_string("{}-{}".format(topic, value))


def main():
    regpub = RegisterPublisher()
    regsub = RegisterSubscriber()
    regpub.start()
    regsub.start()
    while True:
        time.sleep(5)
        print("...")

broker_context = zmq.Context()

# Topic wise registered publishers and subscribers
discovery = dict()

class Broker:
    def __init__(self):


        sub_rep_socket = broker_context.socket(zmq.REP)
        sub_rep_socket.bind("tcp://*:5513")                 # Socket to register request from subscriber
        subscriber_socket = broker_context.socket(zmq.SUB)
        subscriber_socket.bind("tcp://*:5510")              # Socket to received messages from publisher
        publisher_socket = broker_context.socket(zmq.PUB)
        publisher_socket.bind("tcp://*:5511")               # Socket to received messages from subscriber


class RegisterPublisher(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.pub_reg_socket = broker_context.socket(zmq.REP)
        self.pub_reg_socket.bind("tcp://*:5512")                 # Socket to register request from publisher

    def run(self):

        while True:
            print("Register Publisher waiting for message")
            msg = self.pub_reg_socket.recv_string()
            topic, pub_id = msg.split('-')

            if topic not in discovery.keys():
                discovery[topic] = {"publisher": [], "subscriber": []}
                discovery[topic]["publisher"].append(pub_id)
            else:
                discovery[topic]["publisher"].append(pub_id)

            self.pub_reg_socket.send_string("{} successfully registered".format(pub_id))
            print(discovery)
            time.sleep(3)


class RegisterSubscriber(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.sub_reg_socket = broker_context.socket(zmq.REP)
        self.sub_reg_socket.bind("tcp://*:5513")                 # Socket to register request from subscriber

    def run(self):
        # Keeps listening for subscribers
        while True:
            print("Register Subscriber waiting for message")
            msg = self.sub_reg_socket.recv_string()
            topic, sub_id = msg.split('-')

            if topic not in discovery.keys():
                discovery[topic] = {"publisher": [], "subscriber": []}
                discovery[topic]["subscriber"].append(sub_id)
            else:
                discovery[topic]["subscriber"].append(sub_id)

            self.sub_reg_socket.send_string("{} successfully registered".format(sub_id))
            print(discovery)
            time.sleep(3)


class Publish(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.publisher_socket = broker_context.socket(zmq.SUB)
        self.publisher_socket.bind("tcp://*:5510")

    def run(self):
        msg = self.publisher_socket.recv_string()
        topic, value = msg.split('-')
        
        self.queue



