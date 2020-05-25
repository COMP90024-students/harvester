import os
import tweepy
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

AUS_LAT_MIN = -44
AUS_LON_MIN = 110
AUS_LAT_MAX = -9
AUS_LON_MAX = 156

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
    idTweet = tweet['in_reply_to_status_id_str']
    pro_tweet['tweetID_reply'] = tweet['id_str']
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


def getMainText(tweet):
    try:
        text = tweet['full_text']
    except:
        text = tweet['text']
    return text

def tweetProcessor(api_interface):
    for row in db_rep:
        rep = db_rep[row]
        if rep['status'] == 0:
            try:
                statusOne = api_interface.get_status(row, tweet_mode="extended")
                tweetOne = statusOne._json
            except:
                tweetOne = None
            
            try:
                statusTwo = api_interface.get_status(db_rep[row]['tweetID_reply'], tweet_mode="extended")
                tweetTwo = statusTwo._json
            except:
                tweetTwo = None

            if (tweetOne is not None) and (tweetTwo is not None):
                if tweetTwo['in_reply_to_status_id_str'] == tweetOne['id_str']:
                    saveTweetInDatabase(tweetOne)
                    tweetTwo['tweet_replied_to'] = getMainText(tweetOne)
                    saveTweetInDatabase(tweetTwo)
                elif tweetOne['in_reply_to_status_id_str'] == tweetTwo['id_str']:
                    saveTweetInDatabase(tweetTwo)
                    tweetOne['tweet_replied_to'] = getMainText(tweetTwo)
                    saveTweetInDatabase(tweetOne)
            elif (tweetOne is not None) and tweetOne['in_reply_to_status_id_str'] is None:
                saveTweetInDatabase(tweetOne)
            elif (tweetTwo is not None) and tweetTwo['in_reply_to_status_id_str'] is None:
                saveTweetInDatabase(tweetTwo)
        rep['status'] = 1
        db_rep.save(rep)

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
