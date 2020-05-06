import os
import tweepy
import json
import time
import couchdb
import re
from textblob import TextBlob
from nltk.corpus import wordnet

#Local Server
couch = couchdb.Server('http://admin:Cad1020*@127.0.0.1:5984')
global db_hist
global db_rep
global db_quo
db_rep = couch['db_replies']
db_hist = couch['db_historic']
db_quo = couch['db_quoted']

API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

def setCredentials():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)

def saveReply(tweet):
    pro_tweet = {}
    idTweet = tweet['id_str']
    pro_tweet['tweetID_reply'] = tweet['in_reply_to_status_id_str']
    pro_tweet['status'] = 0
    if idTweet not in db_rep:
        db_rep[idTweet] = pro_tweet

def saveQuoted(tweet):
    pro_tweet = {}
    idTweet = tweet['id_str']
    pro_tweet['tweetID_quote'] = tweet['quoted_status_id_str']
    pro_tweet['status'] = 0
    if idTweet not in db_quo:
        db_quo[idTweet] = pro_tweet

def saveTweetInDatabase(tweet):
    idTweet = tweet['id_str']
    if idTweet not in db_hist:
        db_hist[idTweet] = tweet
        print(idTweet)

def tweetProcessor(api_interface):
    try:
        statuses = tweepy.Cursor(api_interface.search,q = 'place:3f14ce28dc7c4566', tweet_mode='extended', exclude_replies=False).items(2000)
        print(statuses)
        for status in statuses:
            tweet = status._json
            print(tweet)
            reply = tweet['in_reply_to_status_id']
            try:
                quote = tweet['quoted_status_id']
            except:
                quote = None

            if reply is not None:
                saveReply(tweet)
            if quote is not None:
                saveQuoted(tweet)
            else:
                saveTweetInDatabase(tweet)
    except:
        time.sleep(600)
        harvestTweets()

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    while True:
        harvestTweets()
        time.sleep(300)
