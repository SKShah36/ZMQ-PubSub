from CS6382 import ToBroker
import sys

Broker_API = ToBroker()
topic = sys.argv[1]
Broker_API.register_sub("{}".format(topic))
Broker_API.notify("{}".format(topic))
