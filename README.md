# ZMQ PubSub Wrapper
This project provides a wrapper around zmq APIs for publish-subscribe application

## About
This is a simplified version built upon zmq which provides easy to use APIs to facilitate development of publish-subscribe
applications. We provide four APIs for the same.

1. register_pub(topic): Registers the publisher with the broker for a topic
2. register_sub(topic): Registers the subscriber with the broker for a topic
3. publish(topic, value): Once registered with a topic, publish values on the topic
4. notify(topic): Calls a callback object when matching topic is found

## Implementation
In our approach the broker is a sole entity that manages incoming messages from the registered
publishers and relays them to the registered subscribers with the topic. 

#### Broker responsibilities:
1. Register requests from publishers and subscribers.
2. Handle incoming messages, provide a discovery service and relay information.
3. Handle heartbeat and manage dead clients.

## Installation
Assuming you have cloned the repository.

- Navigate to root directory
- The project has requirements.txt in the project. To install dependencies:
```
pip3 install -r requirements.txt
```

## How to run?
The main library is called CS6381.py We provide three sample applications: broker.py,
publisher.py and subscriber.py which uses this library:

broker.py: It instantiates an object of class FromBroker from our main library and calls a run method. Code snippet:
```FromBroker_Object.run()```<br/>

To run the broker <br/>
```python3 broker.py```

publisher.py: It imports a ToBroker class which exposes the above mentioned APIs. The publisher
application uses two APIs register_pub and publish to register and publish values over the registered
topic. It accepts a command-ine argument for the topic.

To run the publisher.py:

```python3 publisher.py <topicname>```

subscriber.py: The subscriber application also uses ToBroker class and calls on two APIs, namely
register_sub and notify. It also accepts a command-line argument for the topic.

To run the subscriber.py:

```python3 subscriber.py <topicname>```

config.ini: The configuration is read from this file. You may change IP address and ports depending upon the machine your broker is running.

```Note: ``` Always run the broker application first. Doing otherwise may lead to an unexpected behaviour. 

 ### Test cases
 1. Single Publisher Single Subscriber - The test application that we provide is an implementation of that
 2. Single Subscriber Single Subscriber on different networks (We are using [MiniNet](http://mininet.org/download/))

    ![Alt text](./Tests/ThreeAppsRunning.png?raw=true "ThreeAppsRunning")

    ![Alt text](./Tests/Broker_default.png?raw=true "Broker")

    ![Alt text](./Tests/Publisher1x1.png?raw=true "Publisher")

    ![Alt text](./Tests/Subscriber1x1.png?raw=true "Subscriber")
3. Two publishers in one application single subscriber. The applications are under ```Tests``` folder.
4. Single Publisher - N Subscribers

### Performance Measurement
