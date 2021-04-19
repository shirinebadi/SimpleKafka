# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 13:13:55 2021

@author: Shirin
"""


from kafka import KafkaConsumer
import json
import sys


if __name__ == "__main__":
    consumer = KafkaConsumer("rawdata", group_id = 'group1',bootstrap_servers = ['127.0.0.1:9092'],
      auto_offset_reset = 'earliest')
    
    print("!! New Post !!")
    
    try:
     for message in consumer:
        print (json.loads(message.value))
    except KeyboardInterrupt:
     sys.exit()