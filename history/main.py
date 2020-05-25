import os
import tweepy
import time
import couchdb
from couchdb.http import PreconditionFailed

# configuration
COUCHDB_USER = os.environ.get("COUCHDB_USER", "admin")
COUCHDB_PASSWORD = os.environ.get("COUCHDB_PASSWORD", "admin")
COUCHDB_HOST = os.environ.get("COUCHDB_HOST", "localhost")
COUCHDB_PORT = os.environ.get("COUCHDB_PORT", "5984")
API_KEY = os.environ["TWITTER_API_KEY"]
API_SECRET_KEY = os.environ["TWITTER_API_SECRET_KEY"]
ACCESS_TOKEN = os.environ["TWITTER_ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

couch = couchdb.Server(f'http://{COUCHDB_USER}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCHDB_PORT}')

global db_hist
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


db_hist = create_db(couch, 'db_historic')
db_rep = create_db(couch, 'db_replies')
db_quo = create_db(couch, 'db_quoted')


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
