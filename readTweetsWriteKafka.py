# -*- coding: utf-8 -*-
"""
@author: Hugo Fajardo
This program connects to twitter.com and downloads tweets on a specific topic.
Preprocess received tweets by removing special characters, urls and stopwords.
Finally, it writes the already preprocessed tweets into a Kafka topic.

"""
import tweepy
import json
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from kafka import KafkaProducer

servidor_bootstrap = '192.168.0.2:9092'
topic = 'Twitter'
producer = KafkaProducer(bootstrap_servers=servidor_bootstrap)

consumer_key=''
consumer_secret=''
access_token =''
access_token_secret=''

# Subclass Stream to read Tweets
class MyStream(tweepy.Stream):

    def __init__(self, *args):
        super().__init__(*args)
    
    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        try:
            msg = json.loads( data )
            clean_text = cleanText(msg['text'])
            future = producer.send(topic, clean_text.encode('utf-8'))
            producer.flush()
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True

# Function that receives a text string and preprocesses the received string 
def cleanText(txt):
  txt = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', txt)
  txt = re.sub('[^a-zA-Z]', ' ', txt)
  txt = re.sub('rt', ' ', txt)
  txt = re.sub('RT', ' ', txt)
  txt = " ".join(txt.split())
  txt = txt.lower()
  
  stop_words = set(stopwords.words('english'))
  word_tokens = word_tokenize(txt)
  filtered_sentence = [w for w in word_tokens if not w.lower() in stop_words]
  filtered_sentence = []
  for w in word_tokens:
    if w not in stop_words:
	    filtered_sentence.append(w)
  
  txt = ' '.join(filtered_sentence)
  return txt
  

if __name__ == "__main__":
  # Initialize instance of the subclass
  aStream = MyStream(
        consumer_key, consumer_secret,
        access_token, access_token_secret
    )
  
  # Filter realtime Tweets by keyword
  aStream.filter(track=['soccer'], languages=['en'])