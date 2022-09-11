# %% [markdown]
# # Weather in Australia (Data Stream Version)

# %% [markdown]
# To simulate a unbounded data stream, *0.json* will be loop through infinitely.  
# The producer will publish a meesage in a fixed time interval.  

# %%
# import statements
from time import sleep
from json import dumps
import json
from kafka import KafkaProducer
import datetime as dt
import os

def publish_message(producer_instance, topic_name, data):
    try:
        producer_instance.send(topic_name, data)
    except Exception as ex:
        print('Exception in publishing message.')
        print(str(ex))
        
        
def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                  value_serializer=lambda x: dumps(x).encode('ascii'),
                                  api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer

    
def read_file(path):
    
    with open (path, "r") as f:
        data = json.load(f)
        
    return data
    
    
if __name__ == '__main__':
    
    # Specify the location
    input_location = "dataset"
    file_path = os.path.join(os.getcwd(), input_location, "0.json")
    
    # Read the data
    topic = 'Perth'
    data = read_file(file_path)
    
    print('Publishing records..')
    producer = connect_kafka_producer()
    
    #sending single object
    start_index=len(data) - 1
    
    while True: 
        
        # Loop through the data from the end
        to_be_sent = data[start_index]
        to_be_sent["Time"] = dt.datetime.now().strftime("%X")
        
        publish_message(producer, topic, to_be_sent)
        
        print(to_be_sent)
        print("\n")
        
        # Move to the next meesage
        start_index -= 1
        if start_index == 0: 
            start_index = len(data) - 1
        
        sleep(1)
        

# %%



