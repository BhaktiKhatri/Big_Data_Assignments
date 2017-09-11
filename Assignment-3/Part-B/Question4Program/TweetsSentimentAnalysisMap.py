from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
import json
from _collections import defaultdict

assignment = "HW3"
kbIndex = "sentiment"
topic = 'twitter-stream'

consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
es = Elasticsearch()

if not es.indices.exists(kbIndex):  # create if the index does not exist
    es.indices.create(kbIndex)


mapping = {
    assignment: {
        property: {
                "author":{"type": "string"},
                "sentiment":{"type": "string"},
                "tagType":{"type": "integer"},
                # "polarity":{"type": "string"},
                "date":{"type": "date", "format": "EEE MMM dd HH:mm:ss Z yyyy"},
                "message":{"type": "string", "index": "not_analyzed"},
                "hashtags":{"type": "string", "index": "not_analyzed"},
                "location":{"type": "geo_point", "index": "not_analyzed"},
        }
    }
}

tagTypes = 0
def main():
    analysisdata = SentimentIntensityAnalyzer()
    for msg in consumer:
        tweetdata = json.loads(msg.decode('utf-8'))
        tweettext = tweetdata.get("text")

        if tweetdata.get("place"):
            location = tweetdata.get("place").get("bounding_box").get("coordinates")
            print ("Geo:Latitude: ", location)
            print("Tweet: ", tweettext)
            scorenumber = analysisdata.polarity_scores(tweettext)
            print(scorenumber)
            if scorenumber["compound"]<0:
                sentiment="negative"
            elif scorenumber["compound"] == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"

        tLeft = tweetdata.get("place").get("bounding_box").get("coordinates")[0][0]
        tRight = tweetdata.get("place").get("bounding_box").get("coordinates")[0][2]
        bRight = tweetdata.get("place").get("bounding_box").get("coordinates")[0][1]
        bLeft = tweetdata.get("place").get("bounding_box").get("coordinates")[0][3]
        loc = dict()
        loc["top"] = tLeft
        loc["left"] = bLeft
        loc["bottom"] = bLeft
        loc["right"] = tRight
        print ("elasticSearch: ",loc)

    if "obama" in tweettext:
        tagType = 1
    elif "trump" in tweettext:
        tagType = 2
    content = {"author": tweetdata["user"]["screen_name"],
               "sentiment": sentiment,
               "polarity":scorenumber,
               "date": tweetdata["created_at"],
               "message": tweettext,
               "location": tLeft,
               "tagType": tagType
               }
    es.indices.put_mapping(index=kbIndex, doc_type=assignment, body=mapping)
    es.index(index=kbIndex, doc_type=assignment, body=content)

    es.indices.refresh(index="sentiment")

    if __name__ == "_main_":
        access_token = "853417382173442048-pTUcm10CCcAaM1DZd8HT4BXYReqQBId"
        access_token_secret = "vJl1hya73l3Fz96nOtKJVXgh7KYd6ta68esPSulQgnrU7"
        consumer_key = "eNhfjGU2d80C6XejquPd4yPOz"
        consumer_secret = "cuBs8qNM7GfOJUMGz04ZRQEyaB9Mw8tJ2ZubP2FgF6wAmCNWrL"

        main()