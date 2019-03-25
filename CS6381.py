from threading import Thread
from queue import Queue
from kazoo.client import KazooClient
from kazoo.client import KazooState
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
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
            self.__zk = KazooClient(hosts)
            self.__zk.add_listener(self.__state_listener)
            self.__zk.start()

            try:
                self.__zk.ensure_path("/app/publishers")
                self.__zk.ensure_path("/app/subscribers")

            except kazoo.exceptions.NodeExistsError:
                print("Node exists")

            except kazoo.exceptions.NoNodeError:
                print("There's no such node yet")

        except:
            print("Connection to the zookeeper cannot be established.\nEnsure zookeeper is running.\nExiting...")
            sys.exit(0)

        context = zmq.Context()

        self.__pub_socket = context.socket(zmq.DEALER)
        self.__sub_socket = context.socket(zmq.DEALER)

        self.__broker_ip = ""
        self.__connection_port_pub = ""
        self.__connection_port_sub = ""

        # Set publisher id
        hash_obj_pub = hashlib.md5()
        hash_obj_pub.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.__pub_id = hash_obj_pub.hexdigest()

        # Set subscriber id
        hash_obj_sub = hashlib.md5()
        hash_obj_sub.update("{}{}".format(random.randint(1, 99999), os.getpid()).encode())
        self.__sub_id = hash_obj_sub.hexdigest()

        self.__watch_var = False
        self.__disconnected = False
        self.__pub_var = True
        self.__watch_count = 0

        @self.__zk.DataWatch("/app/broker/leader/ip")
        def watch_node(data, stat):
            print("Watch triggered")
            if self.__zk.exists("/app/broker/leader/ip"):
                if self.__watch_var:

                    try:
                        if not self.__disconnected:
                            self.__pub_socket.disconnect("tcp://{}:{}".format(self.__broker_ip, self.__connection_port_pub))
                            self.__disconnected = True
                        self.__connect2broker(0)

                    except zmq.error.ZMQError:
                        pass

                    try:
                        if not self.__disconnected:
                            self.__sub_socket.disconnect("tcp://{}:{}".format(self.__broker_ip, self.__connection_port_sub))
                            self.__disconnected = True
                        self.__connect2broker(1)
                        self.__send_subscriber_address()
                    except zmq.error.ZMQError:
                        pass

                else:
                    self.__watch_var = True
                    print("Watch Triggered first time")

            else:
                print("No brokers available")
                try:
                    if not self.__disconnected:
                        self.__pub_socket.disconnect("tcp://{}:{}".format(self.__broker_ip, self.__connection_port_pub))
                        self.__disconnected = True

                except zmq.error.ZMQError:
                    pass

                try:
                    if not self.__disconnected:
                        self.__sub_socket.disconnect("tcp://{}:{}".format(self.__broker_ip, self.__connection_port_sub))
                        self.__disconnected = True

                except zmq.error.ZMQError:
                    pass

    # Register publisher with zookeeper
    def register_pub(self, topic, samples=0, ownership_strength=0):
        ownership_strength = int(ownership_strength)
        topic_attributes = [self.__pub_id, samples, ownership_strength]
        if ownership_strength > 999:
            ownership_strength = 999

        if self.__zk.exists("/app/topic/{}".format(topic)):
            current_owner_id, current_samples, current_ownership_strength = self.__resolve_attributes(topic)
            if self.__pub_id == current_owner_id:
                pass
            elif ownership_strength > current_ownership_strength:
                self.__create_publisher_node(topic, topic_attributes)
            else:
                print("Your id {} doesn't own the topic {}. You cannot publish.\n".format(self.__pub_id, topic))
                n = input("Press 0 to exit, 1 to wait\n")
                if n == '0':
                    print("Exiting")
                    sys.exit()
                else:
                    while self.__pub_var:
                        if not self.__zk.exists("/app/topic/{}".format(topic)):
                            self.__create_publisher_node(topic, topic_attributes, 1)
                        time.sleep(2)
        else:
            self.__create_publisher_node(topic, topic_attributes)

        # Create a publisher node

    # Register subscriber with broker
    def register_sub(self, topic, samples=0):
        samples = int(samples)
        current_owner_id, current_samples, current_ownership_strength = self.__resolve_attributes(topic)
        if current_samples >= samples:
            topic_attributes = [self.__sub_id, samples, 0]
            # Create a subscriber node
            try:
                self.__zk.ensure_path("/app/subscribers/{}".format(self.__sub_id))
                self.__zk.create("/app/subscribers/{}/{}".format(self.__sub_id, topic), "{}".format(topic_attributes).encode('utf-8'),
                               ephemeral=True)
                print("Subscriber {} registered for topic {}".format(self.__sub_id, topic))
                self.__connect2broker(1)

            except kazoo.exceptions.NodeExistsError:
                self.__zk.delete("/app/publishers/{}".format(self.__sub_id))
                print("Subscriber has already been registered with the topic".format(topic))

        else:
            raise Exception("The requested history is more than available history.\nPlease, try with a smaller sample size")

    # Publish messages
    def publish(self, topic, value):
        current_owner_id, current_samples, current_ownership_strength = self.__resolve_attributes(topic)

        try:
            # Check if the publish API is called without registration
            if not self.__zk.exists("/app/publishers/{}".format(self.__pub_id)):
                print("Publisher {} isn't registered".format(self.__pub_id))
                sys.exit(0)

            # Check if the publisher is registered with the supplied topic
            elif topic not in self.__zk.get_children("/app/publishers/{}".format(self.__pub_id)):
                print("{} hasn't been registered for the publisher id {}. Register before publishing".format(topic, self.__pub_id))

            # Check if the publisher owns the topic
            elif self.__pub_id != current_owner_id:
                print("Your id {} doesn't own the topic {}. You cannot publish.\n".format(self.__pub_id, topic))
                n = input("Press 0 to exit, 1 to wait\n")
                if n == '0':
                    print("Exiting")
                    sys.exit()
                else:
                    while True:
                        if not self.__zk.exists("/app/topic/{}".format(topic)):
                            topic_attributes = self.__zk.get("/app/publishers/{}/{}".format(self.__pub_id, topic))[0].decode('utf-8')
                            self.__zk.create("/app/topic/{}".format(topic), "{}".format(topic_attributes).encode('utf-8'), ephemeral=True)
                            break
                        time.sleep(2)

            else:
                top_val = "{},{}-{}:{}".format(self.__pub_id, topic, value, time.time())
                print("Publishing {}".format(top_val))
                try:
                    self.__pub_socket.send_string(top_val)

                except zmq.ZMQError:
                    print("Something is wrong with the connection")

        except KeyboardInterrupt:

            self.__zk.delete("/app/publishers/{}".format(self.__pub_id)) # This works but what happens if there's a power failure
            try:
                self.__zk.delete("/app/topic/{}".format(topic))
                print("Interrupt handled. Topic owner node deleted")
            except kazoo.exceptions.NoNodeError:
                print("Nothing to delete")
                pass
            self.__zk.stop()

    def notify(self, topic, callback_func):

        if topic not in self.__zk.get_children("/app/subscribers/{}".format(self.__sub_id)):
            print("Topic not registered so cannot be notified")

        # Connect and send id to notify handler
        self.__send_subscriber_address()
        self.__sub_socket.setsockopt(zmq.RCVTIMEO, 5000)
        count = 0
        current_average = 0

        # Uncomment the following three lines and line 245 to generate logs
        '''complete_path = self.__ensure_dir()
        f = open(complete_path, "w+")
        f.write("Count,Time difference,Running average latency\n")'''

        try:
            while True:
                try:
                    recv_value = self.__sub_socket.recv()
                except zmq.error.Again:
                    # print("Waiting for messages")
                    continue
                recv_value = recv_value.decode('utf-8')
                topic, val = recv_value.split('-')
                val, sent_time = val.split(':')
                # print("Sent time {}".format(sent_time))
                topic_attr_list = self.__zk.get("/app/subscribers/{}/{}".format(self.__sub_id, topic))[0].decode(
                    'utf-8')
                current_owner_id, current_samples, sample_sent = topic_attr_list[1:-1].split(', ')
                current_samples = int(current_samples)
                sample_sent = int(sample_sent)
                if current_samples > 0 and sample_sent == 0:
                    print("History samples are")
                callback_func(topic, val)
                recv_time = time.time()
                print("Received time: {}, Sent time {}".format(recv_time, sent_time))
                time_diff = recv_time - float(sent_time)
            # Calculate average latency
                count = count + 1
                current_average = self.__average_latency(sent_time, recv_time, current_average, count)
                print("Current latency: {}\nTime diff: {}\n".format(current_average, time_diff))
                # f.write("{},{},{}\n".format(count, time_diff, current_average))  #Uncomment to generate logs
                time.sleep(0.5)

        except KeyboardInterrupt:
            # f.close()
            self.__zk.delete("/app/subscribers/{}".format(self.__sub_id))
            self.__zk.stop()

    @staticmethod
    def __state_listener(state):
        if state == KazooState.LOST:
            print("Current state is now = LOST")
        elif state == KazooState.SUSPENDED:
            print("Current state is now = SUSPENDED")
        elif state == KazooState.CONNECTED:
            print("Current state is now = CONNECTED")
        else:
            print("Current state now = UNKNOWN !! Cannot happen")

    def __create_publisher_node(self, topic, topic_attributes, num=0):
        try:
            self.__zk.ensure_path("/app/publishers/{}".format(self.__pub_id))
            self.__zk.create("/app/publishers/{}/{}".format(self.__pub_id, topic), "{}".format(topic_attributes).encode('utf-8'),
                           ephemeral=True)

            self.__zk.ensure_path("/app/topic")

            # If the topic owner already exists then this node should be set to new attributes.
            # This case arises when publisher with higher ownership strength wants to acquire the topic ownership
            if self.__zk.exists("/app/topic/{}".format(topic)):
                self.__zk.delete("/app/topic/{}".format(topic))
                self.__zk.create("/app/topic/{}".format(topic), "{}".format(topic_attributes).encode('utf-8'), ephemeral=True)
            else:
                self.__zk.create("/app/topic/{}".format(topic), "{}".format(topic_attributes).encode('utf-8'), ephemeral=True)
                print("Publisher node created")

            if num == 1:
                self.__pub_var = False
            self.__connect2broker(0)

        except kazoo.exceptions.NodeExistsError:
            self.__zk.delete("/app/publishers/{}".format(self.__pub_id))
            self.__zk.delete("/app/topic/{}".format(topic))
            print("Publisher has already been registered with the topic")

    def __connect2broker(self, i):
        if self.__zk.exists("/app/broker/leader/ip"):
            time.sleep(1) # Give some time to broker to create nodes
            try:
                self.__broker_ip = self.__zk.get("/app/broker/leader/ip")
                self.__broker_ip = self.__broker_ip[0].decode('utf-8')
                print("Current broker ip", self.__broker_ip)

            except kazoo.exceptions.NoNodeError:
                print("There's no broker available right now") # This is redundant

            if i == 0:
                try:
                    self.__connection_port_pub = self.__zk.get("/app/broker/leader/publish_handle")
                    self.__connection_port_pub = self.__connection_port_pub[0].decode('utf-8')
                    print("Current pub port", self.__connection_port_pub)
                    self.__pub_socket.connect("tcp://{}:{}".format(self.__broker_ip, self.__connection_port_pub))

                except kazoo.exceptions.NoNodeError:
                    print("There's no publisher")

            if i == 1:
                try:
                    self.__connection_port_sub = self.__zk.get("/app/broker/leader/subscribe_handle")
                    self.__connection_port_sub = self.__connection_port_sub[0].decode('utf-8')
                    print("Current sub port", self.__connection_port_sub)
                    self.__sub_socket.connect("tcp://{}:{}".format(self.__broker_ip, self.__connection_port_sub))
                    print("Subscriber reconnected")

                except kazoo.exceptions.NoNodeError:
                    print("There's no subscriber")

            self.__disconnected = False # Since connection has been established

        else:
            print("No leaders yet")

    def __send_subscriber_address(self):
        topic = self.__zk.get_children("/app/subscribers/{}".format(self.__sub_id))[0]
        top_id = "{},{}".format(self.__sub_id, topic)
        print("Trying to send {}".format(top_id))
        self.__sub_socket.send_string(top_id)

    def __resolve_attributes(self, topic):
        count = 0
        while count != 100:
            try:
                topic_attr_list = self.__zk.get("/app/topic/{}".format(topic))[0].decode('utf-8')
                count += 1
                break
            except kazoo.exceptions.NoNodeError:
                print("Waiting for a publisher on this topic")
                time.sleep(1)
                continue

        current_owner_id, current_samples, current_ownership_strength = topic_attr_list[1:-1].split(', ')
        current_owner_id = current_owner_id[1:-1]
        current_samples = int(current_samples)
        current_ownership_strength = int(current_ownership_strength)
        return current_owner_id, current_samples, current_ownership_strength

    @staticmethod
    def __ensure_dir():
        test_log_name = "latency_data_1x10"
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

    @staticmethod
    def __average_latency(sent_time, recv_time, current_average, count):
        time_diff = recv_time - float(sent_time)
        average = ((count-1)*current_average + time_diff)/count
        return average


class FromBroker:
    def __init__(self, configuration='config.ini'):
        config = configparser.ConfigParser()
        try:
            config.read(configuration)
            self.__ports = config['PORT']
            self.__ip = config['IP']['BROKER_IP']

        except KeyError:
            print("Invalid configuration file")
            sys.exit(1)

        self.__receiver_socket = ""
        self.__sender_socket = ""
        self.__heartbeat_socket = ""
        self.__queue = ""

        hosts = '127.0.0.1:2181'

        try:
            self.__zk = KazooClient(hosts)
            self.__zk.add_listener(self.__state_listener)
            self.__zk.start()

            try:
                self.__zk.ensure_path("/app/broker/leader")

            except kazoo.exceptions.NodeExistsError:
                print("Node exists")

            except kazoo.exceptions.NoNodeError:
                print("There's no such node yet")

        except:
            print("Connection to the zookeeper cannot be established.\nEnsure zookeeper is running.\nExiting...")
            sys.exit(0)

        self.__wait_var = True

        if self.__zk.exists("/app/broker/leader/ip"):
            @self.__zk.DataWatch("/app/broker/leader/ip")
            def watch_node(data, stat):
                print("Watch triggered")
                print("wait variable {}".format(self.__wait_var))
                if self.__zk.exists("/app/broker/leader/ip") and self.__wait_var:
                    self.__wait_var = True

                else:
                    time.sleep(1)
                    self.__create_broker()

        else:
            pass

        # InfluxDB Management
        try:
            self.__client = InfluxDBClient('127.0.0.1', 8086, None, None, 'History')
        except InfluxDBClientError:
            print("Connection to InfluxDB cannot be established. Please check and try again later")
            sys.exit()

        ## Set retention policy
        try:
            self.__client.create_retention_policy('1hr', duration='1h', database='History')
        except:
            pass
    @staticmethod
    def __state_listener(state):
        if state == KazooState.LOST:
            print("Current state is now = LOST")
        elif state == KazooState.SUSPENDED:
            print("Current state is now = SUSPENDED")
        elif state == KazooState.CONNECTED:
            print("Current state is now = CONNECTED")
        else:
            print("Current state now = UNKNOWN !! Cannot happen")

    def __create_broker(self, events=None):
        try:
            print("Creating a broker")
            self.__zk.create("/app/broker/leader/ip", "{}".format(self.__ip).encode('utf-8'), ephemeral=True)
            self.__zk.create("/app/broker/leader/publish_handle", "{}".format(self.__ports['RECEIVE']).encode('utf-8'), ephemeral=True)
            self.__zk.create("/app/broker/leader/subscribe_handle", "{}".format(self.__ports['SEND']).encode('utf-8'), ephemeral=True)
            print("Broker created with IP {}".format(self.__ip))
            self.__wait_var = False

        except kazoo.exceptions.NodeExistsError:
            print("Broker cannot be created")
            number = random.randint(0, 5)

            while True:
                print("wait variable {}".format(self.__wait_var))
                if self.__zk.exists("/app/broker/leader/ip") and self.__wait_var:
                    print("Waiting {}".format(number))
                    time.sleep(10)
                else:
                    break

    def __setup_broker(self):

        broker_context = zmq.Context()
        self.__receiver_socket = broker_context.socket(zmq.ROUTER)  # Socket to receive messages from publisher
        self.__sender_socket = broker_context.socket(zmq.ROUTER)  # Socket to send messages from publisher to subscriber
        counter = 0
        while counter < 10:
            counter += 1
            try:
                self.__receiver_socket.bind("tcp://*:{}".format(self.__ports['RECEIVE']))
                self.__sender_socket.bind("tcp://*:{}".format(self.__ports['SEND']))

            except zmq.error.ZMQError:
                time.sleep(1)
                continue
        print("Broker setup")
        self.__queue = Queue()

    def __publisher_handler(self):

        while True:
            # print("Waiting for messages in publish handler")
            address, msg = self.__receiver_socket.recv_multipart()
            msg = msg.decode('utf-8')
            print("Received message: {}".format(msg))
            pub_id, top_val = msg.split(',')
            topic, time_val = top_val.split('-')
            value, time = time_val.split(':')

            if self.__zk.exists("/app/publishers/{}/{}".format(pub_id, topic)):
                try:
                    self.__zk.create("/app/publishers/{}/address".format(pub_id), address, ephemeral=True)
                except kazoo.exceptions.NodeExistsError:
                    pass
                except kazoo.exceptions.NoNodeError:
                    time.sleep(1)
                    continue

                self.__queue.put(msg)
                self.__write_influx(topic, value, pub_id)

            else:
                continue

    def __send_history(self):
        if self.__zk.exists("/app/subscribers/"):
            for sub_id in self.__zk.get_children("/app/subscribers"):
                for topic in self.__zk.get_children("/app/subscribers/{}".format(sub_id)):
                    if topic == "address":
                        continue
                    topic_attr_list = self.__zk.get("/app/subscribers/{}/{}".format(sub_id, topic))[0].decode('utf-8')
                    current_sub_id, samples, samples_sent = topic_attr_list[1:-1].split(', ')
                    samples = int(samples)
                    if samples_sent == '0' and int(samples) > 0:
                        print("Samples sent")
                        result = self.__client.query(
                            "SELECT * from history where topic =~ /{}/ order by time desc limit {}".format(topic, samples))
                        result = result.raw
                        result = result['series'][0]['values']
                        for items in result:
                            top_val = "{}-{}:{}".format(items[2], items[3], time.time())
                            if self.__zk.exists("/app/subscribers/{}/address".format(sub_id)):
                                address = self.__zk.get("/app/subscribers/{}/address".format(sub_id))
                                address = address[0]
                                print("Sending on address", address)
                                self.__sender_socket.send_multipart([address, top_val.encode('utf-8')])
                        topic_attributes = [sub_id, samples, 1]
                        self.__zk.set("/app/subscribers/{}/{}".format(sub_id, topic), "{}".format(topic_attributes).encode('utf-8'))

    def __send_handler(self):
        while True:
            self.__send_history()
            msg = self.__queue.get()
            pub_id, top_val = msg.split(',')
            topic, val = top_val.split('-')

            if self.__zk.exists("/app/subscribers/"):
                for sub_id in self.__zk.get_children("/app/subscribers"):
                    if topic in self.__zk.get_children("/app/subscribers/{}".format(sub_id)):
                        print("Topics {}".format(topic))

                        if self.__zk.exists("/app/subscribers/{}/address".format(sub_id)):
                            address = self.__zk.get("/app/subscribers/{}/address".format(sub_id))
                            address = address[0]
                            print("Sending on address", address)
                            self.__sender_socket.send_multipart([address, top_val.encode('utf-8')])

                        else:
                            time.sleep(1)
                            print("No subscriber address node")
                            continue

            else:
                print("No subscribers yet")
                time.sleep(1)
                continue

    def __update_subscriber_address(self):

        while True:
            address, top_id = self.__sender_socket.recv_multipart()
            top_id = top_id.decode('utf-8')
            sub_id, topic = top_id.split(',')
            print("Subs address{}".format(address))
            if self.__zk.exists("/app/subscribers/{}/{}".format(sub_id, topic)):
                print("Subid added")
                try:
                    if self.__zk.exists("/app/subscribers/{}/address".format(sub_id)):
                        print("Address exists. Resetting the address")
                        self.__zk.set("/app/subscribers/{}/address".format(sub_id), address)
                    else:
                        print("Doesn't exist. Recreating the node")
                        self.__zk.create("/app/subscribers/{}/address".format(sub_id), address)
                except kazoo.exceptions.NodeExistsError:
                    print("Cannot use notify more than once")

    def __write_influx(self, topic, value, pub_id):
        json = [
            {
                "measurement": "history",
                "tags": {
                    "publisherId": pub_id
                },
                "fields": {
                    "topic": topic,
                    "value": int(value)
                }
            }
        ]

        self.__client.write_points(json)

    def __resolve_attributes(self, topic):
        try:
            topic_attr_list = self.__zk.get("/app/topic/{}".format(topic))[0].decode('utf-8')
            current_owner_id, current_samples, current_ownership_strength = topic_attr_list[1:-1].split(', ')
            current_owner_id = current_owner_id[1:-1]
            current_samples = int(current_samples)
            current_ownership_strength = int(current_ownership_strength)
            return current_owner_id, current_samples, current_ownership_strength

        except kazoo.exceptions.NoNodeError:
            pass

    def __get_count(self, topic):
        result = self.__client.query("select count(topic) from history where topic =~ /{}/".format(topic))
        result = result.raw
        try:
            count = int(result['series'][0]['values'][0][1])
            print("Samples stored in DB for {} is {}".format(topic, count))
        except KeyError:
            pass
        return count

    def __purge_query(self, topic, threshold):
        result = self.__client.query("SELECT * from history where topic =~ /{}/ order by time desc limit {}".format(topic, threshold),
                              epoch='h|m|s|ms|us|ns')
        result = result.raw
        result = result['series'][0]['values']
        last_time = result[-1][0]
        self.__client.query("delete from history where time < {}".format(last_time))

    def __retention_manager(self):
        while True:
            for topic in self.__zk.get_children("/app/topic"):
                count = self.__get_count(topic)
                publisher_id, sample_count, ownership_strength = self.__resolve_attributes(topic)
                threshold = int(round(1.2*sample_count))

                if count > threshold:
                    self.__purge_query(topic, threshold)
            time.sleep(10)

    def run(self):

        try:
            self.__create_broker()
            self.__setup_broker()
            t1 = Thread(target=self.__publisher_handler)
            t2 = Thread(target=self.__send_handler)
            t3 = Thread(target=self.__update_subscriber_address)
            t4 = Thread(target=self.__retention_manager)

            t1.start()
            t2.start()
            t3.start()
            t4.start()

            while True:
                print("...")
                time.sleep(3)
        except KeyboardInterrupt:
            print("Caught interrupt")
            self.__receiver_socket.close()
            self.__sender_socket.close()
            t1.join()
            t2.join()
            t3.join()
            t4.join()




