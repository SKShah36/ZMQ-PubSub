# ZMQ PubSub Wrapper
This project provides a wrapper around zmq APIs for publish-subscribe application with zookeeper facilities.

## Implementation
In our approach the zookeeper coordinates the communication between the brokers, publishers and subsciribers. From the various brokers, a single broker acquires the lock to create the leader node while the other brokers wait. When the leader broker dies, the zookeeper allows one of the remaining brokers to become the leader. In addition to this the zookeeper assigns ownership strengths to the publishers for each topic to reduce the contention between various publishers publishing on the same topic. For each topic, only the messages published by the maximum ownership strength publisher is relayed to the subscribers. On top of this, the zookeeper also keeps message history on each topic, which the subscriber can demand during registration with the zookeeper. The maximum number of samples available is contingent on the Publisher with the maximum ownership strength for the particular topic.

#### Zookeeper responsibilities:
1. Appoints a leader broker.
2. Maintains meta-data for all the brokers, publishers and subscribers.
3. Uses watch-mechanism to monitor live status of all the nodes.
4. Assign ownership strength to the publishers on each topic.
5. Provide message history to the subscribers on the requested topic.

## Installation
To clone only this branch: 
```git clone -b zookeeper --single-branch https://github.com/SKShah36/ZMQ-PubSub.git ```
Assuming you have cloned the repository.

- Navigate to root directory
- The project has requirements.txt in the project. To install dependencies:
```
pip3 install -r requirements.txt
```

## How to run?
### Start Zookeeper Server

For download and install instructions go to [zookeeper](https://zookeeper.apache.org/releases.html)
- To start the zookeeper navigate to the zookeeper directory (eg. zookeeper-3.4.12)
- ```bin/zkServer.sh start```. This command starts the zookeeper server. Make sure the port in the configuration file is 2181.

### Start InfluxDB

We have used InfluxDB to implement the history service in our application. Use the following instructions to download and install the InfluxDB for Ubuntu: <br/>
```curl -sL https://repos.influxdata.com/influxdb.key | sudo apt-key add -```<br/>
```source /etc/lsb-release```<br/>
```echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/influxdb.list```<br/>

Install InfluxDB:<br/>
```sudo apt-get update && sudo apt-get install influxdb```

Start InfluxDB: <br/>
```sudo service influxdb start```

To connect to and check InfluxDB using commandline:<br/>
```influx```

### Application

The main library is called CS6381.py We provide three sample applications: broker.py,
publisher.py and subscriber.py which uses this library:

broker.py: It instantiates an object of class FromBroker from our main library and calls a run method. Code snippet:
```FromBroker_Object.run()```<br/>

To run the broker <br/>
```python3 broker.py <configuration>```
For e.g. ```python3 broker.py config.ini```  

publisher.py: It imports a ToBroker class which exposes the above mentioned APIs. The publisher
application uses two APIs register_pub and publish to register and publish values over the registered
topic. It accepts a command-ine argument for the topic.

#### Ownership strength
The ownership strength for a topic defines the strength of the publisher for a particular topic. Only one publisher can publish on a particular topic at a time. By default, all publishers have zero ownership strength.<br/>  
You can provide an argument ownership strength=  to register_pub API. <br/>
For e.g. ```register_pub(topic="Temperature", ownership_strength=5)```


#### History QoS

We use a database to maintain the history of assigned samples. The samples can be provided as follows:<br/>
```register_pub(topic="Temperature", samples=5```) or <br/>
```register_sub(topic="Temperature", samples=5```)<br/>

To run the publisher.py:

```python3 publisher.py <topicname> <ownership_strength>```

For e.g. ```python3 publisher.py Temperature 5```


subscriber.py: The subscriber application also uses ToBroker class and calls on two APIs, namely
register_sub and notify. It also accepts a command-line argument for the topic.

To run the subscriber.py:

```python3 subscriber.py <topicname> <samples>```

For e.g. ```python3 subscriber.py Temperature 10```

  
```Note: ``` The subscriber cannot have more samples than what the current publisher owns. By default, publishers and subscribers don't have history enabled.

config.ini: The configuration is read from this file. You may change IP address and ports depending upon the machine your broker is running.

```Note: ``` Always run the broker application first. Doing otherwise may lead to an unexpected behaviour. 


