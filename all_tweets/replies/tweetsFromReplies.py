import tweepy
import time
import couchdb


#Local Server
couch = couchdb.Server('http://admin:Cad1020*@127.0.0.1:5984')
global db_hist
global db_rep
global db_quo
db_hist = couch['db_historic']
db_rep = couch['db_replies']
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
