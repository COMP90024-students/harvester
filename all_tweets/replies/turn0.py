import tweepy
import time
import couchdb


#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')
global db
global db_rep
db_stream = couch['db_stream']
db_rep = couch['db_replies']
db = couch['db_testrep']

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
    idTweet = tweet['in_reply_to_status_id_str']
    pro_tweet['tweetID_reply'] = tweet['id_str']
    pro_tweet['status'] = 0
    if idTweet not in db:
        db[idTweet] = pro_tweet

def saveTweetInDatabase(tweet):
    idTweet = tweet['id_str']
    if idTweet not in db_stream:
        db_stream[idTweet] = tweet
        print(idTweet)

def tweetProcessor(api_interface):
    for row in db_rep:
       rep = db_rep[row]
       statusOne = api_interface.get_status(row, tweet_mode="extended")
       tweetOne = statusOne._json
       saveTweetInDatabase(tweetOne)
       statusTwo = api_interface.get_status(rep['tweetID_reply'], tweet_mode="extended")
       tweetTwo = statusTwo._json
       saveTweetInDatabase(tweetTwo)
       break

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
