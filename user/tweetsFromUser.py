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

TOPIC = ['racism', 'racist', 'coronavirus', 'covid19', 'chinese']


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

    print(pro_tweet)
    #Save json record in couchdb
    if idTweet not in db:
        db[idTweet] = tweet
        print(idTweet)

def tweetProcessor(api_interface):
    next_search_id = 0
    while True:
        try:

            for row in db.view('tweet/userID-view'):
                print(row['key'])
                tweets = tweepy.Cursor(api_interface.user_timeline,id=row['key']).items()
                for tweet in tweets:
                    saveTweetInDatabse(tweet._json)
        except:
            continue
        #time.sleep(5.05)

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
