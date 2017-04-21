#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
#Import kafka for python
import time

from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError
from kafka import KafkaProducer
#Extras 
import subprocess
import os
import pandas as pd
#Variables that contains the user credentials to access Twitter API 
access_token = "227835837-WD07ixlyOeLqkeywbnMYzk5dnebJjd1pA4sKpOjl"
access_token_secret = "6utbaX2ab3UrpL4PpfSx6ToCuuQZgZ5zDDqKQq2albTLL"
consumer_key = "dwazigqjw1ZIVtx2jKGGsw2wb"
consumer_secret = "Lyy1wItpyPTfPWIBJ5d2qrA250s3iydzwKaCubVAamOTOecK2A"

producer = KafkaProducer(bootstrap_servers='localhost:9092')
#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
	#Only text 
        json_load = json.loads(data)
        texts=json_load['text']
        print (texts)
        #delete topic 
        #subprocess.call(['/opt/kafka/bin/kafka-topics.sh', '--delete', '--zookeeper', 'localhost:2181', '--topic', 'streams-wordcount-output'])
        #subprocess.call(['/opt/kafka/bin/kafka-topics.sh', '--delete', '--zookeeper', 'localhost:2181', '--topic', 'streams-wordcount-Counts-changelog'])
        #subprocess.call(['/opt/kafka/bin/kafka-topics.sh', '--delete', '--zookeeper', 'localhost:2181', '--topic', 'streams-wordcount-Counts-repartition'])
        #Send information to topic
        producer.send('streams-file-input', texts.encode())
	#Exectue wordcountdemo
        subprocess.call(['/opt/kafka/bin/kafka-run-class.sh', 'org.apache.kafka.streams.examples.wordcount.WordCountDemo'])
        #Place to delete the topic 
        prueba=pd.read_table('twe_pre.txt', delim_whitespace=True, names=['word', 'count'])
        fin_pru=prueba[prueba["count"] > 2]
       # print(fin_pru)
        fin_pru.to_csv(r'val3.txt', header=None, index=None, sep=' ', mode='a')
       #subprocess.call(['cat', 'val3.txt', '|', '/opt/kafka/bin/kafka-console-producer.sh', '--broker-list', 'localhost:9092', '--topic', 'test'])
        os.system('cat val3.txt | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test') 
        os.system('rm -Rf /tmp/kafka-streams')
        os.system('echo  > twe_pre.txt && echo  > val3.txt')
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
stream.filter(track=['playstation'])
