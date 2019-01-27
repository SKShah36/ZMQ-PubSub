import zmq

context = zmq.Context()

frontend = context.socket(zmq.PUB)
frontend.bind("tcp://*:5512")
print("Frontend bound")

backend = context.socket(zmq.SUB)
backend.bind("tcp://*:5513")
backend.setsockopt(zmq.SUBSCRIBE, b"Temperature")
backend.setsockopt(zmq.SUBSCRIBE, b"Humidity")
print("Backend bound")

zmq.proxy(backend, frontend)


while True:
    data = backend.recv()
    print(data)

