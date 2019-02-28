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
import errno


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

        self.watch_var = False
        self.disconnected = False

        @self.zk.DataWatch("/app/broker/leader/ip")
        def watch_node(data, stat):
            print("Watch triggered")
            if self.zk.exists("/app/broker/leader/ip"):
                if self.watch_var:

                    try:
                        if not self.disconnected:
                            self.pub_socket.disconnect("tcp://{}:{}".format(self.broker_ip, self.connection_port_pub))
                            self.disconnected = True
                        self.connect2broker(0)

                    except zmq.error.ZMQError:
                        pass

                    try:
                        if not self.disconnected:
                            self.sub_socket.disconnect("tcp://{}:{}".format(self.broker_ip, self.connection_port_sub))
                            self.disconnected = True
                        self.connect2broker(1)
                        self.send_subscriber_address()
                    except zmq.error.ZMQError:
                        pass

                else:
                    self.watch_var = True
                    print("Watch Triggered first time")

            else:
                print("No brokers available")
                try:
                    if not self.disconnected:
                        self.pub_socket.disconnect("tcp://{}:{}".format(self.broker_ip, self.connection_port_pub))
                        self.disconnected = True

                except zmq.error.ZMQError:
                    pass

                try:
                    if not self.disconnected:
                        self.sub_socket.disconnect("tcp://{}:{}".format(self.broker_ip, self.connection_port_sub))
                        self.disconnected = True

                except zmq.error.ZMQError:
                    pass

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

        if self.zk.exists("/app/broker/leader/ip"):
            time.sleep(1) # Give some time to broker to create nodes
            try:
                self.broker_ip = self.zk.get("/app/broker/leader/ip")
                self.broker_ip = self.broker_ip[0].decode('utf-8')
                print("Current broker ip", self.broker_ip)

            except kazoo.exceptions.NoNodeError:
                print("There's no broker available right now") # This is redundant

            if i == 0:
                try:
                    self.connection_port_pub = self.zk.get("/app/broker/leader/publish_handle")
                    self.connection_port_pub = self.connection_port_pub[0].decode('utf-8')
                    print("Current pub port", self.connection_port_pub)
                    self.pub_socket.connect("tcp://{}:{}".format(self.broker_ip, self.connection_port_pub))

                except kazoo.exceptions.NoNodeError:
                    print("There's no publisher")

            if i == 1:
                try:
                    self.connection_port_sub = self.zk.get("/app/broker/leader/subscribe_handle")
                    self.connection_port_sub = self.connection_port_sub[0].decode('utf-8')
                    print("Current sub port", self.connection_port_sub)
                    self.sub_socket.connect("tcp://{}:{}".format(self.broker_ip, self.connection_port_sub))
                    print("Subscriber reconnected")

                except kazoo.exceptions.NoNodeError:
                    print("There's no subscriber")

            self.disconnected = False # Since connection has been established

        else:
            print("No leaders yet")

    # Publish messages
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

    def send_subscriber_address(self):
        topic = self.zk.get_children("/app/subscribers/{}".format(self.sub_id))[0]
        top_id = "{},{}".format(self.sub_id, topic)
        print("Trying to send {}".format(top_id))
        self.sub_socket.send_string(top_id)

    def notify(self, topic, callback_func):

        if topic not in self.zk.get_children("/app/subscribers/{}".format(self.sub_id)):
            print("Topic not registered so cannot be notified")

        # Connect and send id to notify handler
        self.send_subscriber_address()
        self.sub_socket.setsockopt(zmq.RCVTIMEO, 5000)
        count = 0
        current_average = 0

        complete_path = self.ensure_dir()

        f = open(complete_path, "w+")
        f.write("Count,Time difference,Running average latency\n")
        try:
            while True:
                try:
                    recv_value = self.sub_socket.recv()
                except zmq.error.Again:
                    # print("Waiting for messages")
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
                time.sleep(0.5)

        except KeyboardInterrupt:
            f.close()
            self.zk.delete("/app/subscribers/{}".format(self.sub_id))
            self.zk.stop()

    def ensure_dir(self):
        test_log_name = "latency_data_10x1"
        path = os.getcwd()
        complete_path = "{}/Performance_Measurement/Performance_Log/{}/{}-{}.csv".format(path, test_log_name,
                                                                                         test_log_name, os.getpid())

        if not os.path.exists(os.path.dirname(test_log_name)):
            try:
                os.makedirs(os.path.dirname(complete_path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        return complete_path

    def average_latency(self, sent_time, recv_time, current_average, count):
        time_diff = recv_time - float(sent_time)
        average = ((count-1)*current_average + time_diff)/count
        return average


class FromBroker:
    def __init__(self, configuration='config.ini'):
        config = configparser.ConfigParser()
        try:
            config.read(configuration)
            self.ports = config['PORT']
            self.ip = config['IP']['BROKER_IP']

        except KeyError:
            print("Invalid configuration file")
            sys.exit(1)

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

        self.wait_var = True

        if self.zk.exists("/app/broker/leader/ip"):
            @self.zk.DataWatch("/app/broker/leader/ip")
            def watch_node(data, stat):
                print("Watch triggered")
                print("wait variable {}".format(self.wait_var))
                if self.zk.exists("/app/broker/leader/ip") and self.wait_var:
                    self.wait_var = True

                else:
                    time.sleep(1)
                    self.create_broker()

        else:
            pass

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
            print("Creating a broker")
            self.zk.create("/app/broker/leader/ip", "{}".format(self.ip).encode('utf-8'), ephemeral=True)
            self.zk.create("/app/broker/leader/publish_handle", "{}".format(self.ports['RECEIVE']).encode('utf-8'), ephemeral=True)
            self.zk.create("/app/broker/leader/subscribe_handle", "{}".format(self.ports['SEND']).encode('utf-8'), ephemeral=True)
            print("Broker created with IP {}".format(self.ip))
            self.wait_var = False

        except kazoo.exceptions.NodeExistsError:
            print("Broker cannot be created")
            number = random.randint(0, 5)

            while True:
                print("wait variable {}".format(self.wait_var))
                if self.zk.exists("/app/broker/leader/ip") and self.wait_var:
                    print("Waiting {}".format(number))
                    time.sleep(10)
                else:
                    break

    def setup_broker(self):

        broker_context = zmq.Context()
        self.receiver_socket = broker_context.socket(zmq.ROUTER)  # Socket to receive messages from publisher
        self.sender_socket = broker_context.socket(zmq.ROUTER)  # Socket to send messages from publisher to subscriber
        counter = 0
        while counter < 10:
            counter += 1
            try:
                self.receiver_socket.bind("tcp://*:{}".format(self.ports['RECEIVE']))
                self.sender_socket.bind("tcp://*:{}".format(self.ports['SEND']))

            except zmq.error.ZMQError:
                time.sleep(1)
                continue
        print("Broker setup")
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
                        print("Topics {}".format(topic))

                        if self.zk.exists("/app/subscribers/{}/address".format(sub_id)):
                            address = self.zk.get("/app/subscribers/{}/address".format(sub_id))
                            address = address[0]
                            print("Sending on address", address)
                            self.sender_socket.send_multipart([address, top_val.encode('utf-8')])

                        else:
                            time.sleep(1)
                            print("No subscriber address node")
                            continue

            else:
                print("No subscribers yet")
                time.sleep(1)
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
                    if self.zk.exists("/app/subscribers/{}/address".format(sub_id)):
                        print("Address exists. Resetting the address")
                        self.zk.set("/app/subscribers/{}/address".format(sub_id), address)
                    else:
                        print("Doesn't exist. Recreating the node")
                        self.zk.create("/app/subscribers/{}/address".format(sub_id), address)
                except kazoo.exceptions.NodeExistsError:
                    print("Cannot use notify more than once")

    def run(self):

        try:
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
        except KeyboardInterrupt:
            print("Caught inteeesdscs")
            self.receiver_socket.close()
            self.sender_socket.close()
            t1.join()
            t2.join()
            t3.join()




