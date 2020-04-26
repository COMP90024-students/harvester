import os
import tweepy
import json
import time
import couchdb

from tweepy.streaming import StreamListener

#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')
global db
db = couch['db_test']

AUS_LAT_MIN = -44
AUS_LON_MIN = 110
AUS_LAT_MAX = -9
AUS_LON_MAX =156

API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        data = json.loads(data)
        processTweet(data)
        print(data)

    def on_error(self, status):
        print("Streaming error status : " + status)

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

def streamTweet(api):
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    try:
        myStream.filter(locations=[AUS_LON_MIN,AUS_LAT_MIN,AUS_LON_MAX,AUS_LAT_MAX], languages=[None, 'und', 'en'], is_async=True)

    except tweepy.RateLimitError:
        print("Error while streaming")


def harvestTweets():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth)
    streamTweet(api)

if __name__ == '__main__':
    harvestTweets()
