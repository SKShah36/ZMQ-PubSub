from CS6381 import ToBroker
import time
import random
import sys
import os

print(os.getpid())

topic = "Temperature"
ownership_strength = 0
if len(sys.argv) > 1:
    topic = "{}".format(sys.argv[1])

if len(sys.argv) > 2:
    ownership_strength = "{}".format(sys.argv[2])
print(ownership_strength)
Broker_API = ToBroker()
Broker_API.register_pub(topic, ownership_strength=ownership_strength, samples=5)
while True:
    Broker_API.publish(topic, random.randint(100, 200))
    time.sleep(5)
