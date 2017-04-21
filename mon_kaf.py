from kafka import KafkaConsumer
from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError
from kafka import KafkaProducer
import json
import pymongo
from pymongo import MongoClient

client = MongoClient('172.17.0.2', 27017)

db = client.grad

def main():
    consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"])
    consumer.subscribe(['kaf_mon'])
    for message in consumer:
        # This will wait and print messages as they become available
        dmes=message.value.decode()
        #json_dump = json.dumps(message.value.decode())
        json_dump =  json.dumps(dmes)
        #json_dump = json.dumps(json_load)
        #texts=json_dump['text']
        #del json_load['text']
        json_load=json.loads(json_dump)
        cursor=db.tweets.insert(json_load)
        print(json_load)
if __name__ == "__main__":
    main()
