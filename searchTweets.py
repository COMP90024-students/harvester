import os
import tweepy
import json
import time
import couchdb

#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')
global db
db = couch['db_test']


API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

def storeDatabase(idTweet, tweet):
    #print(idTweet)
    #if idTweet not in db.view('tweet/tweetID-view')['_id']:
    try:
        db[idTweet] = tweet
    except:
        print(idTweet)
        next

def getPlace(data):
    result = None
    if data['place']:
        result = data['place']['name']
    elif data['user']['location']:
        result = data['user']['location']
    return result

def processTweet(tweet):
    pro_tweet = {}
    if "_rev" in pro_tweet: del pro_tweet["_rev"]
    idTweet = tweet['id_str']
    pro_tweet['username'] = tweet['user']['name']
    pro_tweet['userId'] = tweet['user']['id']
    pro_tweet['created_at'] = tweet['created_at']
    pro_tweet['placeRaw'] = tweet['place']
    pro_tweet['geoRaw'] = tweet['geo']
    pro_tweet['coordinatesRaw'] = tweet['coordinates']
    pro_tweet['place'] = getPlace(tweet)
    if 'text' in tweet:
        pro_tweet['text'] = tweet['text']
    elif 'full_text' in tweet:
        pro_tweet['text'] = tweet['full_text']
    storeDatabase(idTweet, pro_tweet)


def tweetByTopic(api):
    query = "place:3f14ce28dc7c4566  lang:en -filter:retweets"
    maxTweets = 2000
    for tweet in tweepy.Cursor(api.search, q=query,tweet_mode='extended').items(maxTweets):
        processTweet(tweet._json)

def harvestTweets():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth)
    tweetByTopic(api)

if __name__ == '__main__':
    harvestTweets()
