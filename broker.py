from CS6381 import FromBroker
import sys

configuration = 'config.ini'
print(sys.argv[1])
if len(sys.argv) > 0:
    configuration = sys.argv[1]

broker = FromBroker("{}".format(configuration))
broker.run()
