# weather2.0
This project is developed using **python 3.8.2.** in a **Window** system using Oracle Virtual Machine. 

## Purpose
This is improved version of the ![weather project](https://github.com/Jianyin-L/weather)

It calculates
a. the *average temperature* for each city  
b. the *top three most common* "weather text" for each city 

## Improvement
This project contains two aparts: *producer* and *consumer*.  

*Producer* will produce a unbounded data stream to mimic the real life situation.  
To do so, 0.json will be loop through infinitely as an example. It will publish a meesage in a fixed time interval. 

*Consumer* will consumer the unbounded data stream into two forms.
1. The consumer_graph will consume the messages and output two graphs as below. 
![](https://github.com/Jianyin-L/weather2.0/blob/main/result/weather_in_Perth.gif)
2. The consumer_analysis will consume the messages and output two csvs according.  

## Setting up Kafka
Install Kafka by downloading binaries.  
Since it is based on JVM languages like Scala and Java, Java 7 or greater is required.  
Learn more [here](https://kafka.apache.org/quickstart).

## To run the script
1. Install Dependencies

    `pip install -r requirements.txt`

2. To run the producer, run the following command in the terminal

    `python producer.py` 
    
3. To run the consumer, open **another** terminal and run the following command

    `python consumer_analysis.py` 
