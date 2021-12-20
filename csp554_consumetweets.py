from kafka import KafkaConsumer
import json
from json import loads
from pymongo import MongoClient

def refresh_hashtag_count(payload):

    for i in payload:
        value,count = i[0] , i[1]
        print(value,count)
        result = table1.update_one({'hashtag':value},{'$inc':{'count':1}},upsert=True)
        print(result)



def process_hashtags(text):
    try:
        hashtags = text["entities"]["hashtags"]
        if(len(hashtags)>0):
        	hashtags =map(lambda w: (w["text"],1),hashtags)
        	refresh_hashtag_count(hashtags)
    except:
        return False

consumer= KafkaConsumer('twittercoviddata',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',enable_auto_commit=True,group_id='my-group',value_deserializer= (lambda x: json.loads(x.decode('utf-8'))))

client = MongoClient('mongodb+srv://ninad:i5oVP4hzNs4J9eEC@cluster0.h4zod.mongodb.net/natours?retryWrites=true&w=majority')
db=client.get_database('tweetanalysis')
table1 = db.hashtagcount


for message in consumer:
    twitter_message = message.value
    #print(twitter_message)
    process_hashtags(twitter_message)



    
