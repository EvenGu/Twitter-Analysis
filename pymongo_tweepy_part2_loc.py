from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient

MONGO_HOST = 'mongodb://localhost/usa_db'

# source from latitudelongitude.org/us/
# Lat-long coorditates for cities in United States are in range:
# Latitude from 19.50139 to 64.85694 and longitude from -161.75583 to -68.01197.
USA_bounding_box = [-161.75583, 19.50139, -68.01197, 64.85694]
# will get some tweets from Canada and Mexico

# My twitter app keys and access tokens, used for OAuth
consumer_key = 'cm7pWpr799bSyfswZeVR29Uhn'
consumer_secret = 'limCkE2RfELFnhiPMCTglmrcJ2qwi0G4qvtGjDEbrsBQ5GoqWM'
access_token = '213155385-FNC3ZCCrxMYjPQEo50rfPduV5CaVPzgzSt0Ak16R'
access_token_secret = '3JSdGFdTQCZek0dTZJSdoqaz5wX99TSjbcCGGjeZVt33m'

class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is the part that connects to your mongoDB and stores the tweet
        try:
            client = MongoClient(MONGO_HOST)
            db = client.usa_db
            datajson = json.loads(data)
            if datajson['coordinates'] != None:
                coord = datajson['coordinates']['coordinates']
                created_at = datajson['created_at']
                print("Tweet collected at " + str(created_at) + " from " + str(coord))
                # insert the data into the mongoDB into the collection usa_tweets_collection
                db.usa_tweets_collection.insert(datajson)
        except Exception as e:
            print(e)


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
streamer.filter(locations=[-175.1, 22.4, -59.8, 72.3]) #from Mona

