from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError
from kafka import KafkaProducer

import json

import pymongo
from pymongo import MongoClient


import datetime

es = Elasticsearch([{'host': 'search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com', 'port': 80}])
#producer = KafkaProducer(bootstrap_servers='localhost:9092')
client = MongoClient('54.174.5.92', 27017)

db = client.feremi

def main():
    consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"])
    consumer.subscribe(['el_mon'])
    count=1
    for message in consumer:
    	try:
            # This will wait and print messages as they become available
            dmes=message.value.decode()
            now=datetime.datetime.now()
            #es.index(index='feremi', doc_type='kafel', id=now.strftime("%Y-%m-%d %H:%M:%S.%f"), body= dmes)
            es.index(index='feremi', doc_type='kafel', body= dmes)
            #hola=es.get(index='feremi', doc_type='kafel', id=now.strftime("%Y-%m-%d %H:%M:%S.%f"))
            json_load =  json.loads(dmes)
            texts=json_load['text'].split()
            del json_load['text']
            wordfreq = []
            for w in texts:
                wordfreq.append(texts.count(w))
            maw=max(list(zip(wordfreq, texts)))
            json_load['favorita']= maw
            cursor=db.tweets.insert(json_load)
            #nueid=cursor.inserted_id
            print(cursor)
	        #count=count+1
            #hend =str(hola['_source'])
            #producer.send('kaf_mon', str.encode(hend))
    	except:
        	pass
	

if __name__ == "__main__":
    main()

