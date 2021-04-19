# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 11:55:58 2021

@author: Shirin

"""

from kafka import KafkaProducer
import json
import time


def get_post_info(typename,dimension,url,cpation): 
 return{
        "__typename" : typename,
        "dimenstion":dimension,
        "display_url":url,
        "caption": cpation
        }

def json_serializer(data):
 return json.dumps(data).encode("utf-8")

if __name__ == "__main__":
 producer = KafkaProducer(bootstrap_servers = ['127.0.0.1:9092'], value_serializer=json_serializer)
 while 1==1 :
        print("\nSubmit New Post")
        
        val1 = input("Enter the TypeName: ")
        val2 = input("Enter the dimension: ")
        val3 = input("Enter the url: ")
        val4 = input("Enter the caption: ")
        registered_data = get_post_info(val1,val2,val3,val4)
        producer.send("rawdata",registered_data)
        time.sleep(5)
    
