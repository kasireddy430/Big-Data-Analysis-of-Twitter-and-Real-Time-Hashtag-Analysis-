
#Imports
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials
from pykafka import KafkaClient
import json

def get_kafka_client():
	return KafkaClient(hosts='127.0.0.1:9092')

class StdOutListener(StreamListener):

	def on_data(self, data):
		'''message = json.loads(data)
								print(message["entities"]["hashtags"])
								text = message["text"]
								if "extended_tweet" in message:
									text = message["extended_tweet"]["full_text"]
								
								text = json.dumps({"text":text})'''
		client = get_kafka_client()
		topic = client.topics['twittercoviddata']
		producer = topic.get_sync_producer()
		producer.produce(data.encode('utf-8'))
		return True

	def on_error(self,status):
		print(status)


if __name__ == '__main__':

	auth = OAuthHandler(credentials.CONSUMER_KEY,credentials.CONSUMER_SECRET)
	auth.set_access_token(credentials.ACCESS_TOKEN,credentials.ACCESS_TOKEN_SECRET)

	listener = StdOutListener()
	stream = Stream(auth,listener)
	stream.filter(track=["corona virus","COVID-19","pandemic","vaccine"],languages=["en"])






  