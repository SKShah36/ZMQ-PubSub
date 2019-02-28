from CS6381 import ToBroker
import time
import random
import sys
import os

print(os.getpid())

topic = "Temperature"
if len(sys.argv) > 1:
    topic = "{}".format(sys.argv[1])

Broker_API = ToBroker()

Broker_API.register_pub(topic)
while True:
    Broker_API.publish(topic, random.randint(0, 100))
    time.sleep(5)
