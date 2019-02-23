from threading import Thread, Lock
from queue import Queue
from kazoo.client import KazooClient
from kazoo.client import KazooState
import kazoo
import time
import zmq
import hashlib
import random
import os
import configparser
import sys


class ToBroker:
    def __init__(self):
        hosts = '127.0.0.1:2181'

        try:
            self.zk = KazooClient(hosts)
            self.zk.add_listener(self.state_listener)
            self.zk.start()

            try:
                self.zk.ensure_path("/app/publishers")
                self.zk.ensure_path("/app/subscribers")

            except kazoo.exceptions.NodeExistsError:
                print("Node exists")

            except kazoo.exceptions.NoNodeError:
                print("There's no such node yet")

        except:
            print("Connection to the zookeeper cannot be established.\nEnsure zookeeper is running.\nExiting...")
            sys.exit(0)

        context = zmq.Context()

        self.pub_socket = context.socket(zmq.DEALER)
        self.sub_socket = context.socket(zmq.DEALER)

        self.broker_ip = ""
        self.connection_port_pub = ""
        self.connection_port_sub = ""

        # Set publisher id
        hash_obj_pub = hashlib.md5()
        hash_obj_pub.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.pub_id = hash_obj_pub.hexdigest()

        # Set subscriber id
        hash_obj_sub = hashlib.md5()
        hash_obj_sub.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.sub_id = hash_obj_sub.hexdigest()

    def state_listener(self, state):
        if state == KazooState.LOST:
            print("Current state is now = LOST")
        elif state == KazooState.SUSPENDED:
            print("Current state is now = SUSPENDED")
        elif state == KazooState.CONNECTED:
            print("Current state is now = CONNECTED")
        else:
            print("Current state now = UNKNOWN !! Cannot happen")

    # Register publisher with zookeeper
    def register_pub(self, topic):
        # Create a publisher node
        try:
            self.zk.ensure_path("/app/publishers/{}".format(self.pub_id))
            self.zk.create("/app/publishers/{}/{}".format(self.pub_id, topic), "{}".format(topic).encode('utf-8'),
                           ephemeral=True)
            print("Subscriber has already been registered with the topic".format(topic))
            self.connect2broker(0)

        except kazoo.exceptions.NodeExistsError:
            self.zk.delete("/app/publishers/{}".format(self.pub_id))
            print("Publisher has already been registered with the topic")

    # Register subscriber with broker
    def register_sub(self, topic):

        # Create a subscriber node
        try:
            self.zk.ensure_path("/app/subscribers/{}".format(self.sub_id))
            self.zk.create("/app/subscribers/{}/{}".format(self.sub_id, topic), "{}".format(topic).encode('utf-8'),
                           ephemeral=True)
            print("Subscriber {} registered for topic {}".format(self.pub_id, topic))
            self.connect2broker(1)

        except kazoo.exceptions.NodeExistsError:
            self.zk.delete("/app/publishers/{}".format(self.sub_id))
            print("Subscriber has already been registered with the topic".format(topic))

    def connect2broker(self, i):
        try:
            self.broker_ip = self.zk.get("/app/broker/leader/ip")
            self.broker_ip = self.broker_ip[0].decode('utf-8')
            print("Current broker ip", self.broker_ip)

            self.connection_port_pub = self.zk.get("/app/broker/leader/publish_handle")
            self.connection_port_pub = self.connection_port_pub[0].decode('utf-8')
            print("Current pub port", self.connection_port_pub)

            self.connection_port_sub = self.zk.get("/app/broker/leader/subscribe_handle")
            self.connection_port_sub = self.connection_port_sub[0].decode('utf-8')

        except kazoo.exceptions.NoNodeError:
            print("There's no broker available right now")
        if i == 0:
            self.pub_socket.connect("tcp://{}:{}".format(self.broker_ip, self.connection_port_pub))
        elif i == 1:
            self.sub_socket.connect("tcp://{}:{}".format(self.broker_ip, self.connection_port_sub))

    # Connect and publish messages
    def publish(self, topic, value):
        try:
            if not self.zk.exists("/app/publishers/{}".format(self.pub_id)):
                print("Publisher {} isn't registered".format(self.pub_id))
                sys.exit(0)
            elif topic not in self.zk.get_children("/app/publishers/{}".format(self.pub_id)):
                print("{} hasn't been registered for the publisher id {}. Register before publishing".format(topic, self.pub_id))

            else:
                top_val = "{},{}-{}:{}".format(self.pub_id, topic, value, time.time())
                print("Publishing {}".format(top_val))
                try:
                    self.pub_socket.send_string(top_val)

                except zmq.ZMQError:
                    print("Something is wrong with the connection")

        except KeyboardInterrupt:
            self.zk.delete("/app/publishers/{}".format(self.pub_id)) # This is working but what happens if there's a power failure
            self.zk.stop()

    def watch_broker(self, events=None):
        self.pub_socket.disconnect("tcp://{}:{}".format(self.broker_ip, self.connection_port_pub))
        self.sub_socket.disconnect("tcp://{}:{}".format(self.broker_ip, self.connection_port_sub))

        self.connect2broker(0)
        self.connect2broker(1)

    # Check for notification from broker, if any
    def publish_helper(self):
        while True:
            msg_from_broker = self.pub_socket.recv_string()
            print(msg_from_broker)
            time.sleep(5)

    def notify(self, topic, callback_func):

        # Connect and send id to notify handler
        top_id = "{},{}".format(self.sub_id, topic)
        self.sub_socket.send_string(top_id)

        self.sub_socket.setsockopt(zmq.RCVTIMEO, 5000)
        count = 0
        current_average = 0

        f = open("latency_data_1x10-{}.csv".format(os.getpid()), "w+")
        f.write("Count,Time difference,Running average latency\n")
        try:
            while True:
                print("Waiting for messages")
                try:
                    recv_value = self.sub_socket.recv()
                except zmq.error.Again:
                    continue
                recv_value = recv_value.decode('utf-8')
                topic, val = recv_value.split('-')
                val, sent_time = val.split(':')
                callback_func(topic, val)
                recv_time = time.time()
                print("Received time: {}, Sent time {}".format(recv_time, sent_time))
                time_diff = recv_time - float(sent_time)
            # Calculate average latency
                count = count + 1
                current_average = self.average_latency(sent_time, recv_time, current_average, count)
                print("Current latency: {}\nTime diff: {}\n".format(current_average, time_diff))
                f.write("{},{},{}\n".format(count, time_diff, current_average))
                time.sleep(2)

        except KeyboardInterrupt:
            f.close()
            self.zk.delete("/app/subscribers/{}".format(self.sub_id))
            self.zk.stop()

    def heartbeat(self, identity):
        while True:
            self.heartbeat_socket.send_string("{}".format(identity))
            time.sleep(10)

    def average_latency(self, sent_time, recv_time, current_average, count):
        time_diff = recv_time - float(sent_time)
        average = ((count-1)*current_average + time_diff)/count
        return average


class FromBroker:
    def __init__(self, my_ip='127.0.0.1'):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.ports = config['PORT']
        self.ip = config['IP']['BROKER_IP']

        self.receiver_socket = ""
        self.sender_socket = ""
        self.heartbeat_socket = ""
        self.queue = ""

        hosts = '127.0.0.1:2181'

        try:
            self.zk = KazooClient(hosts)
            self.zk.add_listener(self.state_listener)
            self.zk.start()

            try:
                self.zk.ensure_path("/app/broker/leader")

            except kazoo.exceptions.NodeExistsError:
                print("Node exists")

            except kazoo.exceptions.NoNodeError:
                print("There's no such node yet")

        except:
            print("Connection to the zookeeper cannot be established.\nEnsure zookeeper is running.\nExiting...")
            sys.exit(0)

    def state_listener(self, state):
        if state == KazooState.LOST:
            print("Current state is now = LOST")
        elif state == KazooState.SUSPENDED:
            print("Current state is now = SUSPENDED")
        elif state == KazooState.CONNECTED:
            print("Current state is now = CONNECTED")
        else:
            print("Current state now = UNKNOWN !! Cannot happen")

    def create_broker(self, events=None):
        try:
            self.zk.create("/app/broker/leader/ip", "{}".format(self.ip).encode('utf-8'), ephemeral=True)
            self.zk.create("/app/broker/leader/publish_handle", "{}".format(self.ports['RECEIVE']).encode('utf-8'), ephemeral=True)
            self.zk.create("/app/broker/leader/subscribe_handle", "{}".format(self.ports['SEND']).encode('utf-8'), ephemeral=True)
            print("Broker created with IP {}".format(self.ip))

        except kazoo.exceptions.NodeExistsError:
            self.zk.exists("/app/broker/leader/ip", watch=self.create_broker)

    def setup_broker(self):

        broker_context = zmq.Context()

        self.receiver_socket = broker_context.socket(zmq.ROUTER)  # Socket to receive messages from publisher
        self.receiver_socket.bind("tcp://*:{}".format(self.ports['RECEIVE']))

        self.sender_socket = broker_context.socket(zmq.ROUTER)  # Socket to send messages from publisher to subscriber
        self.sender_socket.bind("tcp://*:{}".format(self.ports['SEND']))

        self.queue = Queue()

    def publisher_handler(self):

        while True:
            # print("Waiting for messages in publish handler")
            address, msg = self.receiver_socket.recv_multipart()
            msg = msg.decode('utf-8')
            print("Received message: {}".format(msg))
            pub_id, top_val = msg.split(',')
            topic, value = top_val.split('-')

            if self.zk.exists("/app/publishers/{}/{}".format(pub_id, topic)):
                try:
                    self.zk.create("/app/publishers/{}/address".format(pub_id), address, ephemeral=True)
                except kazoo.exceptions.NodeExistsError:
                    pass
                except kazoo.exceptions.NoNodeError:
                    time.sleep(1)
                    continue

                self.queue.put(msg)

            else:
                continue

    def send_handler(self):

        while True:

            msg = self.queue.get()
            pub_id, top_val = msg.split(',')
            topic, val = top_val.split('-')

            if self.zk.exists("/app/subscribers/"):
                for sub_id in self.zk.get_children("/app/subscribers"):
                    if topic in self.zk.get_children("/app/subscribers/{}".format(sub_id)):

                        if self.zk.exists("/app/subscribers/{}/address".format(sub_id)):
                            address = self.zk.get("/app/subscribers/{}/address".format(sub_id))
                            address = address[0]
                            print("Sending on address", address)
                            self.sender_socket.send_multipart([address, top_val.encode('utf-8')])

                        else:
                            time.sleep(1)
                            continue

            else:
                print("No subscribers yet")
                time.sleep(2)
                continue

    def update_subscriber_address(self):

        while True:
            address, top_id = self.sender_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            sub_id, topic = top_id.split(',')
            print("Subs address{}".format(address))
            if self.zk.exists("/app/subscribers/{}/{}".format(sub_id, topic)):
                print("Subid added")
                try:
                    self.zk.create("/app/subscribers/{}/address".format(sub_id), address, ephemeral=True)
                except kazoo.exceptions.NodeExistsError:
                    print("Cannot use notify more than once")

    def run(self):

        self.create_broker()
        self.setup_broker()
        t1 = Thread(target=self.publisher_handler)
        t2 = Thread(target=self.send_handler)
        t3 = Thread(target=self.update_subscriber_address)

        t1.start()
        t2.start()
        t3.start()

        while True:
            print("...")
            time.sleep(3)

        t1.join()
        t2.join()
        t3.join()



