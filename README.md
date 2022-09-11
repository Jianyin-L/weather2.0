# weather2.0
This project is developed using **python 3.8.2.** in a **Window** system using Oracle Virtual Machine. 

## Purpose
This is improved version of the ![weather project](https://github.com/Jianyin-L/weather)

It calculates
a. the *average temperature* for each city  
b. the *top three most common* "weather text" for each city 

To simulate a unbounded data stream, 0.json will be loop through infinitely.  
* The producer will publish a meesage in a fixed time interval.  
* The consumer_graph will consume the messages and output two graphs as below. 
![](https://github.com/Jianyin-L/weather2.0/blob/main/result/weather_in_Perth.gif)
* The consumer_analysis will consume the messages and output two csvs according.  

## Setting up Kafka
The easiest way to install Kafka is to download binaries and run it. Since it’s based on JVM languages like Scala and Java, Java 7 or greater is required. Know more [here](https://kafka.apache.org/quickstart).

## To run the script
1. Install Dependencies

    `pip install -r requirements.txt`

2. Run the following command in your terminal

    `python weather_proj.py` 
