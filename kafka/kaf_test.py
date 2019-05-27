from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler

consumer_key =  'ywQ4VIRqZlyILoVU97Holy7D7'
consumer_secret = '2xjiXFXzxdTv0d7CecXPEBhj2daLVbr5g25GBT7JRbbWzXyu6S'
access_token = '1121610787213025281-ULsMOX3t2hgOpYCfFs7LNSf2yfKEtI'
access_token_secret =  'EvKvxHO7AJI9Y7pNlBzPXeo6nzeJuZR2cEhbB5bmXo6D9'

class KafkaListener(StreamListener):
    def on_data(self, streamData):
        producer.send_messages("twitterStream", streamData.encode('utf-8'))
        print (streamData)
        return True
    def on_error(self, status):
        print (status)



kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
listener = KafkaListener()
stream = Stream(auth, listener)
stream.filter(track="twitterStream")
