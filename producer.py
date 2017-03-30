import time
 
from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError
from kafka import KafkaProducer 
import sys

args=sys.argv
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for _ in range(10):
	producer.send('test', args[1].encode())
