from CS6382 import ToBroker

Broker_API = ToBroker()

Broker_API.register_sub("Temperature")
Broker_API.notify("Temperature")
