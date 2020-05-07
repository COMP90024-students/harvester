import tweepy
import json
import time
import couchdb
from tweepy.streaming import StreamListener

# Local Server
couch = couchdb.Server('http://admin:admin*@115.146.95.10:5984')
global db_stream
global db_rep
global db_quo
db_stream = couch['db_streamer']
db_rep = couch['db_replies']
db_quo = couch['db_quoted']

API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

AUS_LAT_MIN = -44
AUS_LON_MIN = 110
AUS_LAT_MAX = -9
AUS_LON_MAX = 156


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
