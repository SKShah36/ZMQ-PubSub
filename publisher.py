from CS6381 import ToBroker
import time
import sys

Broker_API = ToBroker()

Broker_API.register_pub("Temperature")
while True:
    Broker_API.publish("Temperature", 15)
    time.sleep(5)
