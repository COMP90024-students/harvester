import os
import tweepy
import json
import time

'''
API_KEY = os.environ['CONSUMER_KEY']
API_SECRET_KEY = os.environ['CONSUMER_SECRET']
ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = os.environ['ACCESS_TOKEN_SECRET']
print(API_KEY)
'''
API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

AUS_LAT_MIN = 0
AUS_LAT_MAX = 0
AUS_LON_MIN = 0
AUS_LON_MAX = 0

database = ''
UserDb_Design = 'userId'
UserDb_View = 'userId-view'

SEARCH_TOPIC = ["coronavirus"]

def isValidLoc(longitude,latitude):
    if not longitude:
        return False
    if not latitude:
        return False
    if latitude >= AUS_LAT_MIN and latitude <= AUS_LAT_MAX and longitude >= AUS_LON_MIN and longitude <= AUS_LON_MAX :
        return True
    return False

def processTweet(tweet):
    print(tweet)

def tweetByTopic(api):
    SEARCH_QUERY_STRING = "place:3f14ce28dc7c4566 coronavirus -filter:retweets"
    print("Search for str = " + str(SEARCH_QUERY_STRING))
    maxTweets = 2000
    count = 0
    for tweet in tweepy.Cursor(api.search, q=SEARCH_QUERY_STRING,tweet_mode='extended').items(maxTweets):
        processTweet(tweet)
    print("Done: Tweets added: " + str(count))

def tweetStream(api):
    next_search_id = 0
    while True:
        SEARCH_QUERY_STRING = "place:3f14ce28dc7c4566  coronavirus -filter:retweets"
        status = api.search(q=SEARCH_QUERY_STRING,
                geocode="-37.814124,144.963913,10km",
                since_id=next_search_id,
                count=100)
        try:
            next_results = status.get("search_metadata").get("next_results")
            next_search_id = next_results.replace('&', '=').split('=')[1]
        except:
            # No more next page
            print('Break at: ', status.get("search_metadata"))
            continue

        print(next_search_id)
        time.sleep(5.05)

def tweetUser(api):
    for row in database.view(UserDb_Design+'/'+UserDb_View):
        userDoc = database[row['id']]
        if userDoc['status'] == 0:
            userDoc['status'] = 1
            database.save(userDoc)

def harvestTweets():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())
    #tweetByTopic(api)
    streamByQuery(api)

if __name__ == '__main__':
    harvestTweets()
