from pymongo import MongoClient
import re
from textblob import TextBlob

client = MongoClient('localhost', 27017)
db = client['twitterdb']
collection = db['twitter_search']

tweets_iterator = collection.find()
'''
for tweet in tweets_iterator:
  print('tweet text: ',tweet['text'])
  print('user\'s screen name: ',tweet['user']['screen_name'])
  print('user\'s name: ',tweet['user']['name'])
  try:
    print('retweet count: ',tweet['retweeted_status']['retweet_count'])
    print('retweeter\'s name: ', tweet['retweeted_status']['user']['name'])
    print('retweeted\'s screen name: ', tweet['retweeted_status']['user']['screen_name'])
  except KeyError:
      pass
'''

# QA: Find the number of tweets that have "data"(case insensitive)
# somewhere in the tweet’s text
regex = re.compile("data", re.IGNORECASE)
QA_iterator = collection.find({"text":regex})
print(QA_iterator.count())

# QB: From all the data related objects, how many of them are geo_enabled?
QB_count_true_itr = collection.find({"$and":[{"text":regex},{"user.geo_enabled": True}]})
print(QB_count_true_itr.count())
'''
QB_count_false = 0
for tweet in QA_iterator:
    if tweet['user']['geo_enabled'] == False:
        QB_count += 1
print(QB_count_false)
'''

# QC: For all the data related tweets, use the TextBlob Python library
# to detect if the Tweet’s sentiment is “Positive”, “Neutral”, or “Negative”.
for tweet in QA_iterator:
    testimonial = TextBlob(tweet['text'])
    if testimonial.sentiment.polarity == 0:
        print('Neutral sentiment for the tweet: ',tweet['text'])
    elif testimonial.sentiment.polarity < 0:
        print('Negative sentiment for the tweet: ', tweet['text'])
    elif testimonial.sentiment.polarity > 0:
        print('Positive sentiment for the tweet: ',tweet['text'])