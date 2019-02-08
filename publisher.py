from CS6382 import ToBroker
import random
import time
import sys

Broker_API = ToBroker()
topic1 = sys.argv[1]
# topic2 = sys.argv[2]
Broker_API.register_pub("{}".format(topic1))
# Broker_API.register_pub("{}".format(topic2))
while True:
    Broker_API.publish("{}".format(topic1), random.randint(1, 1000))
    # Broker_API.publish("{}".format(topic2), random.randint(1, 1000))
    time.sleep(5)
