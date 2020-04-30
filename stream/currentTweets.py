import os
import tweepy
import json
import time
import couchdb
import re
from textblob import TextBlob

from tweepy.streaming import StreamListener

#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')
global db
db = couch['db_test']

API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

POSITIVE_SENTIMENT = 1
NEGATIVE_SENTIMENT = -1
NEUTRAL_SENTIMENT = 0

AUS_LAT_MIN = -44
AUS_LON_MIN = 110
AUS_LAT_MAX = -9
AUS_LON_MAX =156

TOPIC_ONE = ['covid app', 'covidsafe', 'safe app', 'safeapp', 'tracing app', 'tracking app', 'covid safe app']
TOPIC_TWO = ['scomo', 'morrison', 'government', 'gov', 'jobseeker', 'jobkeeper', 'unemployment', 'minister', 'parliament', 'corruption', 'pm', 'premier', 'victorians', 'govt']
TOPIC_THREE = ['app']


class MyStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        data = json.loads(data)
        saveTweetInDatabse(data)

    def on_error(self, status):
        print("Streaming error")

def setCredentials():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)

def inTopic(tweet):
    tweet = tweet.split()
    flag_one = False
    flag_two = False
    flag_three = False
    number = 0
    for word in TOPIC_ONE:
        if word in tweet:
            flag_one = True
            break
    for word in TOPIC_TWO:
        if word in tweet:
            flag_two = True
            break
    for word in TOPIC_THREE:
        if word in tweet:
            flag_three = True
            break
    if flag_three == True:
        number = 4
    if flag_one == True and flag_two == True:
        number = 3
    elif flag_one == False and flag_two == True:
        number = 2
    elif flag_one == True and not flag_two == False:
        number = 1
    else:
        number = 0
    return number

def cleanTweet(tweet):
    tweet = re.sub(r"(?:\@|https?\://)\S+", '', tweet, flags=re.MULTILINE)
    tweet = re.sub('[^A-Za-z0-9]+', ' ', tweet)
    tweet = tweet.lower()
    return tweet

def getSentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return POSITIVE_SENTIMENT
    elif sentiment < 0:
        return NEGATIVE_SENTIMENT
    return NEUTRAL_SENTIMENT

def saveTweetInDatabse(tweet):
    pro_tweet = {}
    #Determines fields to save in json record
    idTweet = tweet['id_str']

    pro_tweet['creation_date'] = tweet['created_at']
    pro_tweet['text'] = cleanTweet(tweet['text'])
    pro_tweet['tweet_sentiment'] = getSentiment(pro_tweet['text'])

    pro_tweet['userId'] = tweet['user']['id_str']
    pro_tweet['username'] = tweet['user']['name']
    pro_tweet['screen_name'] = tweet['user']['screen_name']
    pro_tweet['user_location'] = tweet['user']['location']
    pro_tweet['followers_count'] = tweet['user']['followers_count']
    pro_tweet['geo_enabled'] = tweet['user']['geo_enabled']

    pro_tweet['place'] = tweet['place']
    pro_tweet['geo'] = tweet['geo']
    pro_tweet['coordinates'] = tweet['coordinates']
    pro_tweet['topic'] = inTopic(pro_tweet['text'])
    #Save json record in couchdb
    if idTweet not in db and pro_tweet['topic'] > 0:
        db[idTweet] = pro_tweet
        print(idTweet)

def tweetProcessor(api):
    my_stream_listener = MyStreamListener()
    my_stream = tweepy.Stream(auth = api.auth, listener=my_stream_listener)
    try:
        my_stream.filter(locations=[AUS_LON_MIN,AUS_LAT_MIN,AUS_LON_MAX,AUS_LAT_MAX], languages=[None, 'und', 'en'], is_async=True)
    except tweepy.RateLimitError:
        print("Error")
def harvestTweets():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
