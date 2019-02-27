from CS6381 import FromBroker
import sys

configuration = 'config.ini'

if len(sys.argv) > 1:
    configuration = sys.argv[1]

broker = FromBroker("{}".format(configuration))
broker.run()
