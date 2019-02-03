from threading import Thread
from queue import Queue
import time
import zmq
import hashlib


class ToBroker:
    def __init__(self):
        context = zmq.Context()
        self.pub_socket = context.socket(zmq.DEALER)
        self.sub_socket = context.socket(zmq.DEALER)
        self.req_socket = context.socket(zmq.DEALER)
        self.rep_socket = context.socket(zmq.REP)
        self.pub_id = ""
        self.sub_id = ""

    # Register publisher with broker
    def register_pub(self, topic):
        # Set publisher id
        hash_obj = hashlib.md5()
        hash_obj.update("tcp://127.0.0.1:5512".encode())
        self.pub_id = hash_obj.hexdigest()

        # Connect and send registration message to broker
        self.req_socket.connect("tcp://127.0.0.1:5512")
        self.req_socket.send_string("{}-{}".format(topic, self.pub_id))

        # Receive registration confirmation
        return_msg = self.req_socket.recv()
        return_msg = return_msg.decode('utf-8')
        print("Return message: {}".format(return_msg))

    # Register subscriber with broker
    def register_sub(self, topic):
        # Set subscriber id
        hash_obj = hashlib.md5()
        hash_obj.update("tcp://127.0.0.1:5512".encode())
        self.sub_id = hash_obj.hexdigest()

        # Connect and send registration message to broker
        self.req_socket.connect("tcp://127.0.0.1:5513")
        self.req_socket.send_string("{}-{}".format(topic, self.sub_id))

        # Receive registration confirmation
        return_msg = self.req_socket.recv()
        return_msg = return_msg.decode('utf-8')
        print("Return message: {}".format(return_msg))

    # Connect and publish messages
    def publish(self, topic, value):
        self.pub_socket.connect("tcp://127.0.0.1:5510")
        top_val = "{},{}-{}".format(self.pub_id, topic, value)
        print("Publishing {}".format(top_val))
        self.pub_socket.send_string(top_val)

    # Check for notification from broker, if any
    def publish_helper(self):
        while True:
            msg_from_broker = self.pub_socket.recv_string()
            print(msg_from_broker)
            time.sleep(5)

    def notify(self, topic):

        # Connect and send id to notify handler
        self.sub_socket.connect("tcp://127.0.0.1:5511")
        top_id = "{},{}".format(self.sub_id, topic)
        self.sub_socket.send_string(top_id)

        # Start a thread that receives messages sent by the broker asynchronously
        notify_poll_thread = Thread(target=self.notify_poll())
        notify_poll_thread.daemon = True
        notify_poll_thread.start()

    # Poll for messages from broker
    def notify_poll(self):
        while True:
            print("Waiting for messages")
            recv_value = self.sub_socket.recv()
            recv_value = recv_value.decode('utf-8')
            topic, val = recv_value.split('-')
            print("Topic: {}, Value: {}".format(topic, val))

    # def heartbeat(self):
        # Sometihng


class FromBroker:

    def __init__(self):
        broker_context = zmq.Context()
        self.pub_reg_socket = broker_context.socket(zmq.ROUTER)   # Socket to register request from publisher
        self.pub_reg_socket.bind("tcp://*:5512")
        self.sub_reg_socket = broker_context.socket(zmq.ROUTER)   # Socket to register request from subscriber
        self.sub_reg_socket.bind("tcp://*:5513")
        self.sender_socket = broker_context.socket(zmq.ROUTER)    # Socket to send messages from publisher to subscriber
        self.sender_socket.bind("tcp://*:5511")
        self.receiver_socket = broker_context.socket(zmq.ROUTER)  # Socket to receive messages from publisher
        self.receiver_socket.bind("tcp://*:5510")
        self.discovery = {"publishers": {}, "subscribers": {}}
        self.queue = Queue()

    # def heartbeat_handler(self):

    def register_publisher_handler(self):

        while True:
            print("Register Publisher waiting for message")
            address, top_id = self.pub_reg_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            topic, pub_id = top_id.split('-')
            if pub_id not in self.discovery["publishers"].keys():
                self.discovery["publishers"][pub_id] = {"address": address, "topics": []}
                self.discovery["publishers"][pub_id]["topics"].append(topic)
            else:
                self.discovery["publishers"][pub_id]["topics"].append(topic)

            print(self.discovery)
            self.pub_reg_socket.send_multipart(
                [address, "{} is successfully registered".format(address).encode('utf-8')])
            # self.pub_reg_socket.send_multipart([, ])
            time.sleep(3)

    def register_subscriber_handler(self):
        # Keeps listening for subscribers
        while True:
            print("Register Subscriber waiting for message")
            address, top_id = self.sub_reg_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            topic, sub_id = top_id.split('-')

            if sub_id not in self.discovery["subscribers"].keys():
                self.discovery["subscribers"][sub_id] = {"address": address, "topics": []}
                self.discovery["subscribers"][sub_id]["topics"].append(topic)
            else:
                self.discovery["subscribers"][sub_id]["topics"].append(topic)

            self.sub_reg_socket.send_multipart(
                [address, "{} is successfully registered".format(address).encode('utf-8')])
            print(self.discovery)
            time.sleep(3)

    def receive_handler(self):

        while True:
            print("Inside receive thread")
            address, msg = self.receiver_socket.recv_multipart()
            msg = msg.decode('utf-8')
            pub_id, top_val = msg.split(',')
            topic, value = top_val.split('-')
            print("Received message: {}".format(msg))
            if pub_id not in self.discovery["publishers"].keys():
                print("Publisher isnt registeste")
                self.receiver_socket.send_multipart([address, b'Publisher isnt registered. Register before publishing'])
            elif pub_id in self.discovery["publishers"].keys() and topic not in self.discovery["publishers"][pub_id]["topics"]:
                print("Publisher isn't registered with the given topic")
            else:
                self.discovery["publishers"][pub_id]["address"] = address
                self.queue.put(top_val)

    def send_handler(self):

        while True:
            print("Inside send thread")
            top_val = self.queue.get()
            print(" I found something in thr queue called {}".format(top_val))
            topic, val = top_val.split('-')
            print("Topic {}".format(topic))
            print("Value {}".format(topic))
            print("List of subids {}".format(self.discovery["subscribers"].keys()))

            if self.discovery["subscribers"].keys():
                for sub_id in self.discovery["subscribers"].keys():

                    if topic in self.discovery["subscribers"][sub_id]["topics"]:
                        address = self.discovery["subscribers"][sub_id]["address"]
                        print("Stuck inside send handler before send multipart")
                        self.sender_socket.send_multipart([address, top_val.encode('utf-8')])

            else:
                print("No subscribers yet")
                continue

    def update_subscriber_address(self):

        while True:
            address, top_id = self.sender_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            sub_id, topic = top_id.split(',')
            if sub_id in self.discovery["subscribers"].keys() and topic in self.discovery["subscribers"][sub_id]["topics"]:
                self.discovery["subscribers"][sub_id]["address"] = address

    def run(self):

        t1 = Thread(target=self.register_publisher_handler)
        t2 = Thread(target=self.register_subscriber_handler)
        t3 = Thread(target=self.receive_handler)
        t4 = Thread(target=self.send_handler)
        t5 = Thread(target=self.update_subscriber_address)
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        while True:
            print("...")
            time.sleep(3)
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()



