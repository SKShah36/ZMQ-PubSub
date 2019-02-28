# ZMQ PubSub Wrapper
This project provides a wrapper around zmq APIs for publish-subscribe application with zookeeper facilities.

## Implementation
In our approach the zookeeper coordinates the communication between the brokers, publishers and subsciribers. From the various brokers, a single broker acquires the lock to create the leader node while the other brokers wait. When the leader broker dies, the zookeeper allows one of the remaining brokers to become the leader.

#### Zookeeper responsibilities:
1. Appoints a leader broker.
2. Maintains meta-data for all the brokers, publishers and subscribers.
3. Uses watch-mechanism to monitor live status of all the nodes.

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

To run the publisher.py:

```python3 publisher.py <topicname>```
For e.g. ```python3 publisher.py Temperature```

subscriber.py: The subscriber application also uses ToBroker class and calls on two APIs, namely
register_sub and notify. It also accepts a command-line argument for the topic.

To run the subscriber.py:

```python3 subscriber.py <topicname>```

config.ini: The configuration is read from this file. You may change IP address and ports depending upon the machine your broker is running.

```Note: ``` Always run the broker application first. Doing otherwise may lead to an unexpected behaviour. 

### Performance Test

#### Latency
We calculate average latency vs message count(100 in each case) across three different configurations:
1. Single publisher - Ten subscribers

    ![Alt text](./Performance_Measurement/Performance_Log/latency_data_1x10/1x10.png?raw=true "CountvLatency-1x10") 
       
2. Single publisher - Hundred Subscribers

     ![Alt text](./Performance_Measurement/Performance_Log/latency_data_1x100/1x100.png?raw=true "CountvLatency-1x100") 
    
3. Ten publishers - single subscriber

     ![Alt text](./Performance_Measurement/Performance_Log/latency_data_10x1/10x1.png?raw=true "CountvLatency-10x1") 
     
4. Ten publishers - Ten subscribers

     ![Alt text](./Performance_Measurement/Performance_Log/latency_data_10x10/10x10.png?raw=true "CountvLatency-10x10")
    
##### Observations
1. In the first graph for single publisher and ten subscribers we can observe that with number of subscribers greater than that of publishers latency drops sharply and continues to decrease gradually from there on.
2. In the second graph for single publisher and hundred subscribers we can observe that as initially there are jitters but once the message count increases and the application stabilizes, the latency gradually decreases and hence the curve smoothens out.
3. From the third graph for ten publishers and single subscribers it is evident that the latency increases linearly as the message count increases.
4. From the fourth graph for ten publishers and ten subscribers it is evident that the latency increases linearly as the message count increases.

This behaviour is analogous to the High Production - Low Consumption Problem.

### Future work
##### Performance Measurement
   We plan to add several other performance monitoring parameters such as CPU utilization, Latency v/s Publisher and Latency v/s Subscriber to better gauge the performance of our application.

##### Ownership Strength
   The information from the publisher with the highest ownership strength gets relayed to the subscribers.
##### History
   Store the last N-messages published on a topic.
