from CS6381 import ToBroker
import sys
import os

print(os.getpid())

def message_handler(topic, value):
    print("In message handler callback\nTopic: {}, Value: {}".format(topic, value))


topic = "Temperature"
if len(sys.argv) > 1:
    topic = "{}".format(sys.argv[1])

Broker_API = ToBroker()
Broker_API.register_sub(topic)

Broker_API.notify("{}".format(topic), message_handler)
print("After Notify")  # This should not happen
