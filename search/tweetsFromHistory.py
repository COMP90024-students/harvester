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

QUERY_GET_TWEETS = "place:3f14ce28dc7c4566  lang:en -filter:retweets"

def setCredentials():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)

def getPlace(data):
    result = None
    if data['place']:
        result = data['place']['name']
    elif data['user']['location']:
        result = data['user']['location']
    return result

def saveTweetInDatabse(tweet):
    pro_tweet = {}
    #Determines fields to save in json record
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

    #Save json record in couchdb
    try:
        db[idTweet] = pro_tweet
    except:
        print(idTweet)
        next


def tweetProcessor(api_interface):
    next_search_id = 0
    while True:
        try:
            tweets = api_interface.search(q = QUERY_GET_TWEETS, since_id=next_search_id,count=100)
            for tweet in tweets:
                print(tweet._json)
                saveTweetInDatabse(tweet._json)

            next_results = tweets['search_metadata']["next_results"]
            # Replace '&' with '=' and split string by '='
            next_search_id = next_results.replace('&', '=').split('=')[1]
        except:
            #print(tweets['search_metadata'])
            continue
        time.sleep(5.05)


def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
