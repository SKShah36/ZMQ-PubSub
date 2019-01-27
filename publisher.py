import CS6381
import os

print(os.getpid())
CS6381.register_pub("Temperature", os.getpid())
CS6381.publish("Temperature", 15)
CS6381.register_sub("Temperature", os.getpid())
