#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
#Import kafka for python
import time


import subprocess

#Variables that contains the user credentials to access Twitter API 
access_token = "227835837-WD07ixlyOeLqkeywbnMYzk5dnebJjd1pA4sKpOjl"
access_token_secret = "6utbaX2ab3UrpL4PpfSx6ToCuuQZgZ5zDDqKQq2albTLL"
consumer_key = "dwazigqjw1ZIVtx2jKGGsw2wb"
consumer_secret = "Lyy1wItpyPTfPWIBJ5d2qrA250s3iydzwKaCubVAamOTOecK2A"

#subprocess.call(['/opt/kafka/bin/kafka-console-producer.sh', '--broker-list', 'localhost:9092', '--topic', 'test'])

class StdOutListener(StreamListener):

    def on_data(self, data):
        #Only text 
        json_load = json.loads(data)
        texts=json_load['text'].split()
	
        print(texts,"\n\n")
        
        del json_load["text"]
        print (json_load)
        
        wordfreq = []
        for w in texts:
            wordfreq.append(texts.count(w))

        maw=max(list(zip(wordfreq, texts)))
        json_load['nuevo']= maw
        print("\n\n",json_load)

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

