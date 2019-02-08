from CS6381 import ToBroker
import sys


def message_handler1(topic, value):
    print("In message handler callback\nTopic: {}, Value: {}".format(topic, value))


Broker_API = ToBroker()
topic1 = sys.argv[1]
# topic2 = sys.argv[2]
Broker_API.register_sub("{}".format(topic1))
# Broker_API.register_sub("{}".format(topic2))
Broker_API.notify("{}".format(topic1), message_handler1)
print("After Notify")  # This should not happen
