from CS6382 import ToBroker

Broker_API = ToBroker()

Broker_API.register_pub("Temperature")
Broker_API.register_pub("Humidity")
Broker_API.publish("Temperature", 15)
