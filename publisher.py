from CS6382 import ToBroker
import random
import time
import sys

Broker_API = ToBroker()
topic = sys.argv[1]
# Broker_API.register_pub("Temperature")
Broker_API.register_pub("{}".format(topic))
while True:
    Broker_API.publish("{}".format(topic), random.randint(1, 1000))
    time.sleep(5)
