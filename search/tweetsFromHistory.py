import os
import tweepy
import json
import time
import couchdb
import re
from textblob import TextBlob
from nltk.corpus import wordnet

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

QUERY_PLACE = "place:3f14ce28dc7c4566 "
QUERY_LANGUAGE = "lang:en "
QUERY_RETWEET_FLAG = "-filter:retweets"

TOPIC_ONE = '(COVID AND app) OR (covid AND app) OR (Covidsafe AND app) OR (COVIDSafe AND app) OR (safe AND app) OR (tracing AND app) OR (CovidSafe AND app) OR (PM and app) OR (tracking AND app)'
TOPIC_TWO = '(scomo) OR (Scott AND Morrison) OR (Goverment)'
#

def setTopics():
    QUERY_TOPIC = []
    QUERY_TOPIC.append(TOPIC_ONE)
    QUERY_TOPIC.append(TOPIC_TWO)
    return QUERY_TOPIC

def setCredentials():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)

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

def saveTweetInDatabse(tweet, number):
    pro_tweet = {}
    #Determines fields to save in json record
    idTweet = tweet['id_str']

    pro_tweet['creation_date'] = tweet['created_at']
    pro_tweet['text'] = cleanTweet(tweet['full_text'])
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

    pro_tweet['topic'] = number

    #Save json record in couchdb
    #if idTweet not in db:
    try:
        print(idTweet, pro_tweet )
        db[idTweet] = pro_tweet
    except:
        next
    return tweet['id']


def tweetProcessor(api_interface):
    next_search_id = 0
    last = 0
    topics = setTopics()
    for topic in topics:
        query = QUERY_PLACE + topic + QUERY_LANGUAGE + QUERY_RETWEET_FLAG
        if topic == TOPIC_ONE:
            number = 1
        else:
            number = 2
        for tweet in tweepy.Cursor(api_interface.search, q=query,tweet_mode='extended').items(1000):
            print(tweet._json)
            saveTweetInDatabse(tweet._json, 1)

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
