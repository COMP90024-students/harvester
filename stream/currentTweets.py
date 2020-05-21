import tweepy
import json
import time
import couchdb
from couchdb.http import PreconditionFailed
from tweepy.streaming import StreamListener
import os

# configuration
COUCHDB_USER = os.environ.get("COUCHDB_USER", "admin")
COUCHDB_PASSWORD = os.environ.get("COUCHDB_PASSWORD", "admin")
COUCHDB_HOST = os.environ.get("COUCHDB_HOST", "localhost")
COUCHDB_PORT = os.environ.get("COUCHDB_PORT", "5984")
API_KEY = os.environ["TWITTER_API_KEY"]
API_SECRET_KEY = os.environ["TWITTER_API_SECRET_KEY"]
ACCESS_TOKEN = os.environ["TWITTER_ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

AUS_LAT_MIN = -44
AUS_LON_MIN = 110
AUS_LAT_MAX = -9
AUS_LON_MAX = 156

couch = couchdb.Server(f'http://{COUCHDB_USER}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCHDB_PORT}')

global db_stream
global db_rep
global db_quo


def create_db(client, name):
    """ create a database with given name or return existing database.
    """
    try:
        db = client.create(name)
    except PreconditionFailed:
        db = client[name]
    
    return db


db_stream = create_db(couch, 'db_streamer')
db_rep = create_db(couch, 'db_replies')
db_quo = create_db(couch, 'db_quoted')


class MyStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        tweet = json.loads(data)
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

    def on_error(self, status):
        print("Streaming error")


def setCredentials():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)


def saveReply(tweet):
    pro_tweet = {}
    idTweet = tweet['in_reply_to_status_id_str']
    pro_tweet['tweetID_reply'] = tweet['id_str']
    pro_tweet['status'] = 0
    if idTweet not in db_rep:
        db_rep[idTweet] = pro_tweet


def saveQuoted(tweet):
    pro_tweet = {}
    idTweet = tweet['quoted_status_id_str']
    pro_tweet['tweetID_quote'] = tweet['id_str']
    pro_tweet['status'] = 0
    if idTweet not in db_quo:
        db_quo[idTweet] = pro_tweet


def saveTweetInDatabase(tweet):
    idTweet = tweet['id_str']
    if idTweet not in db_stream:
        db_stream[idTweet] = tweet
        print(idTweet)


def tweetProcessor(api):
    my_stream_listener = MyStreamListener()
    my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)
    try:
        my_stream.filter(locations=[AUS_LON_MIN, AUS_LAT_MIN, AUS_LON_MAX, AUS_LAT_MAX], languages=[None, 'und', 'en'],is_async=True)
    except:
        time.sleep(10)
        harvestTweets()

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
