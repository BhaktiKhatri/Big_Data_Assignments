# import required libraries
from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import time
import tweepy


#Variables that contains the user credentials to access Twitter API
access_token = "853349131938607104-ezHRNTkcGevFThuoJtpBnJR0DYHJ6Mf"
access_token_secret = "pjZZ1mgAozzdKA2hqv32QbjdmjDmlpfic4ep1jymHFMkK"
consumer_key = "1nQ1eXbweQGLuXGo0sdc9FW1P"
consumer_secret = "AgIMZZY0YL3vensd9AIzkCdkhP76D8mFNGXjEqFEmT7oYban64"


# Kafka settings
topic = 'twitter-stream'
# setting up Kafka producer
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)


#This is a basic listener that just put received tweets to kafka cluster.
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        try:
            pdata = json.loads(data)

            print("Screen Name:", pdata.get("user").get("screen_name"))
            if pdata.get("place"):
                producer.send_messages(topic,data.encode('utf-8'))
                print("Geo:Latitude", pdata.get("place").get("bounding_box").get("coordinates")[0])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        print(status_code)

    def on_timeout(self):
        print(self)

    #WORDS_TO_TRACK = "the to and is in it you of for on my that at with me do have just this be so are not was but out up what now new from your like good no get all about we if time as day will one how can some an am by going they go or has know today there love more work too got he back think did when see really had great off would need here thanks been still people who night want why home should well much then right make last over way does getting watching its only her post his morning very she them could first than better after tonight our again down news man looking us tomorrow best into any hope week nice show yes where take check come fun say next watch never bad free life".split()

if __name__ == '__main__':
    print ('running the twitter-stream python code')
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = tweepy.Stream(auth,l)
    # Goal is to keep this process always going
    while True:
        try:
           # stream.sample()
            stream.filter(languages=["en"], track=['#trump','#obama'])
        except:
            pass
