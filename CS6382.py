from threading import Thread, Lock
from queue import Queue
import time
import zmq
import hashlib
import random
import os
import configparser


class ToBroker:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.ports = config['PORT']
        self.ip = config['IP']

        context = zmq.Context()

        self.pub_socket = context.socket(zmq.DEALER)
        self.pub_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['RECEIVE']))

        self.sub_socket = context.socket(zmq.DEALER)
        self.sub_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['SEND']))

        self.reqp_socket = context.socket(zmq.DEALER)
        self.reqp_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['REGISTER_PUBLISHER']))

        self.reqs_socket = context.socket(zmq.DEALER)
        self.reqs_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['REGISTER_SUBSCRIBER']))

        self.heartbeat_socket = context.socket(zmq.DEALER)
        self.heartbeat_socket.connect("tcp://{}:{}".format(self.ip['BROKER_IP'], self.ports['HEARTBEAT']))

        self.pub_id = ""
        self.sub_id = ""

    # Register publisher with broker
    def register_pub(self, topic):
        # Set publisher id
        hash_obj = hashlib.md5()
        hash_obj.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.pub_id = hash_obj.hexdigest()

        # Connect and send registration message to broker
        self.reqp_socket.send_string("{}-{}".format(topic, self.pub_id))

        # Setup heartbeat thread
        heartbeat_thread = Thread(target=self.heartbeat, args=(self.pub_id,))
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # Receive registration confirmation
        return_msg = self.reqp_socket.recv()
        return_msg = return_msg.decode('utf-8')
        print("Return message: {}".format(return_msg))

    # Register subscriber with broker
    def register_sub(self, topic):
        # Set subscriber id
        hash_obj = hashlib.md5()
        hash_obj.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.sub_id = hash_obj.hexdigest()

        # Connect and send registration message to broker
        self.reqs_socket.send_string("{}-{}".format(topic, self.sub_id))

        # Setup heartbeat thread
        heartbeat_thread = Thread(target=self.heartbeat, args=(self.sub_id,))
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # Receive registration confirmation
        return_msg = self.reqs_socket.recv()
        return_msg = return_msg.decode('utf-8')
        print("Return message: {}".format(return_msg))

    # Connect and publish messages
    def publish(self, topic, value):
        top_val = "{},{}-{}:{}".format(self.pub_id, topic, value, time.time())
        print("Publishing {}".format(top_val))
        self.pub_socket.send_string(top_val)

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

    def heartbeat(self, identity):
        while True:
            self.heartbeat_socket.send_string("{}".format(identity))
            time.sleep(10)

    def average_latency(self, sent_time, recv_time, current_average, count):
        time_diff = recv_time - float(sent_time)
        average = ((count-1)*current_average + time_diff)/count
        return average


class FromBroker:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        ports = config['PORT']
        broker_context = zmq.Context()
        self.pub_reg_socket = broker_context.socket(zmq.ROUTER)   # Socket to register request from publisher
        self.pub_reg_socket.bind("tcp://*:{}".format(ports['REGISTER_PUBLISHER']))
        self.sub_reg_socket = broker_context.socket(zmq.ROUTER)   # Socket to register request from subscriber
        self.sub_reg_socket.bind("tcp://*:{}".format(ports['REGISTER_SUBSCRIBER']))
        self.sender_socket = broker_context.socket(zmq.ROUTER)    # Socket to send messages from publisher to subscriber
        self.sender_socket.bind("tcp://*:{}".format(ports['SEND']))
        self.receiver_socket = broker_context.socket(zmq.ROUTER)  # Socket to receive messages from publisher
        self.receiver_socket.bind("tcp://*:{}".format(ports['RECEIVE']))
        self.heartbeat_socket = broker_context.socket(zmq.ROUTER)
        self.heartbeat_socket.bind("tcp://*:{}".format(ports['HEARTBEAT']))
        self.discovery = {"publishers": {}, "subscribers": {}, "id": {}}
        self.queue = Queue()

    def register_publisher_handler(self):

        while True:
            print("Register Publisher waiting for message")
            address, top_id = self.pub_reg_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            topic, pub_id = top_id.split('-')
            self.discovery["publishers"][pub_id] = {"address": address, "topic": topic}
            self.discovery["id"][pub_id] = time.time()
            # print(self.discovery)
            self.pub_reg_socket.send_multipart(
                [address, "{} is successfully registered".format(pub_id).encode('utf-8')])
            time.sleep(3)

    def register_subscriber_handler(self):
        # Keeps listening for subscribers
        while True:
            print("Register Subscriber waiting for message")
            address, top_id = self.sub_reg_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            topic, sub_id = top_id.split('-')

            self.discovery["subscribers"][sub_id] = {"address": address, "topic": topic}
            self.discovery["id"][sub_id] = time.time()
            print(self.discovery)
            self.sub_reg_socket.send_multipart(
                [address, "{} is successfully registered".format(address).encode('utf-8')])
            time.sleep(3)

    def receive_handler(self):

        while True:
            address, msg = self.receiver_socket.recv_multipart()
            msg = msg.decode('utf-8')
            pub_id, top_val = msg.split(',')
            print("Received message: {}".format(msg))
            if pub_id not in self.discovery["publishers"].keys():
                print("Publisher isn't registered")
                self.receiver_socket.send_multipart([address, b"Publisher isn't registered. Register before publishing"])
            else:
                self.discovery["publishers"][pub_id]["address"] = address
                self.queue.put(top_val)

    def send_handler(self):

        while True:
            if self.discovery["subscribers"]:
                top_val = self.queue.get()
                topic, val = top_val.split('-')

                for sub_id in self.discovery["subscribers"].keys():
                    if topic in self.discovery["subscribers"][sub_id]["topic"]:
                        address = self.discovery["subscribers"][sub_id]["address"]
                        self.sender_socket.send_multipart([address, top_val.encode('utf-8')])

            else:
                print("No subscribers yet")
                time.sleep(5)
                continue

    def update_subscriber_address(self):

        while True:
            address, top_id = self.sender_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            sub_id, topic = top_id.split(',')
            if sub_id in self.discovery["subscribers"].keys() and topic in self.discovery["subscribers"][sub_id]["topic"]:
                self.discovery["subscribers"][sub_id]["address"] = address

    def is_client_dead(self):
        while True:
            if self.discovery["id"]:
                delete_list = []
                lock = Lock()
                lock.acquire()
                try:
                    for identity in self.discovery["id"].keys():
                        last_heartbeat = self.discovery["id"][identity]
                        time_diff = int(round(time.time()-last_heartbeat))

                        if time_diff > 60:
                            delete_list.append(identity)
                finally:
                    lock.release()
                for identity in delete_list:
                    try:
                        self.discovery["publishers"].pop(identity)
                    except KeyError:
                        pass
                    try:
                        self.discovery["subscribers"].pop(identity)
                    except KeyError:
                        pass
                    try:
                        self.discovery["id"].pop(identity)
                    except KeyError:
                        pass
                    print("{} is dead. Removed!".format(identity))

            else:
                print("There's no client alive")
                time.sleep(5)

    def update_heartbeat(self):
            address, heartbeat_id = self.heartbeat_socket.recv_multipart()
            heartbeat_recv_time = time.time()
            heartbeat_id = heartbeat_id.decode('utf-8')

            # Check and update last heartbeat
            if heartbeat_id in self.discovery["id"].keys():
                self.discovery["id"][heartbeat_id] = heartbeat_recv_time

    def run(self):

        t1 = Thread(target=self.register_publisher_handler)
        t2 = Thread(target=self.register_subscriber_handler)
        t3 = Thread(target=self.receive_handler)
        t4 = Thread(target=self.send_handler)
        t5 = Thread(target=self.update_subscriber_address)
        t6 = Thread(target=self.is_client_dead)
        t7 = Thread(target=self.update_heartbeat)
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t6.start()
        t7.start()
        while True:
            print("...")
            time.sleep(3)
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
        t6.join()
        t7.join()



