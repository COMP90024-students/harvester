import os
import tweepy
import json
import time
import couchdb
import re
from textblob import TextBlob
from nltk.corpus import wordnet

#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')
global db
global db_rep
db = couch['db_test']
db_rep = couch['db_replies']

API_KEY = 'Y9QPnFuofPJDysYtfq3OJrIlp'
API_SECRET_KEY = '4KLD9lHOWVzG6TUmEORZ2gxW4kanXtsECoj0RIxu1Udnix4bpj'
ACCESS_TOKEN = '1031024454-17u1rln5FQWmh8DcJnCHbKMEZwOtqaxQCx9l2ac'
ACCESS_TOKEN_SECRET = 'K3GhuJzCVgwrzM1g1PQ5LxCjYqR8Pssy3FV1zMZPIcGkC'

POSITIVE_SENTIMENT = 1
NEGATIVE_SENTIMENT = -1
NEUTRAL_SENTIMENT = 0

QUERY_PLACE = "place:3f14ce28dc7c4566 "
QUERY_LANGUAGE = " lang:en "
QUERY_RETWEET_FLAG = "-filter:retweets"

#TOPIC_ONE = '(COVID AND app) OR (covid AND app) OR (Covidsafe AND app) OR (COVIDSafe AND app) OR (safe AND app) OR (tracing AND app) OR (CovidSafe AND app) OR (PM and app) OR (tracking AND app)'
#TOPIC_TWO = '(scomo) OR (Morrison) OR (Goverment) OR (morrison) OR (gov OR Gov OR Govt) OR (Jobseeker OR jobseeker) OR (Jobkeeper OR jobkeeper) OR unemployment OR minister OR parliament OR corruption OR (PM OR PM) OR premier OR victorians OR economy OR @DanielAndrewsMP OR @ScottMorrisonMP'
#TOPIC_THREE = 'app OR downloaded'
'''
TOPIC_ONE = ['(COVID AND App) OR (COVID AND app)',
             'covid19 AND app OR COVID19 app OR Covid19 app',
             '(Covid and App) OR (Covid AND app)',
             '(covid AND App) OR (covid AND app)',
             'Covidsafe OR covidsafe OR COVIDSafe',
             '(safe AND app) OR (Safe AND app),'
             '(morrison AND app) OR (Morisson AND app) OR (@ScottMorrisonMP AND app) OR (@DanielAndrewsMP AND app)',
             '(tracing AND app) OR (tracking AND app)',
             'coronavirus AND app']
'''

TOPIC_ONE = ['(covid AND app)',
             'Covidsafe OR covidsafe OR COVIDSafe',
             '(safe AND app) OR (Safe AND app),'
             '(morrison AND app) OR (Morisson AND app) OR (@ScottMorrisonMP AND app) OR (@DanielAndrewsMP AND app)',
             '(tracing AND app) OR (tracking AND app)',
             'coronavirus AND app']

TOPIC_TWO = ['gov OR Gov OR Govt or goverment OR Goverment',
             'ScoMo OR scomo OR Morrison OR morrison OR PM OR pm OR Pm OR premier OR Premier',
             '@ScottMorrisonMP',
             'Jobseeker OR jobseeker OR Jobkeeper OR jobkeeper OR (Job AND Keeper) OR (JOB AND KEEPER)'
             'minister OR Minister OR parliament OR Parliament',
             'economy',
             'victorians',
             '@DanielAndrewsMP',
             'politicians', 'politics',
             'health OR Health',
             'school OR schools OR School OR Schools',
             'Dan Tehan OR DanTehanWannon OR @DanTehanWannon',
             'Jenny Mikakos OR JennyMikakos OR @JennyMikakos OR mimakos OR Mikakos',
             'governor', 'governors',
             'teacher OR teachers OR Teacher OR Teachers',
             '@ServicesGovAU OR ServicesGovAU']

TOPIC_THREE = 'app OR downloaded'

def setTopics():
    QUERY_TOPIC = []
    QUERY_TOPIC.append(TOPIC_ONE)
    QUERY_TOPIC.append(TOPIC_TWO)
    QUERY_TOPIC.append(TOPIC_THREE)
    return QUERY_TOPIC

def setCredentials():
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)

def cleanTweet(tweet):
    tweet = re.sub(r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+", '', tweet, flags=re.MULTILINE)
    tweet = re.sub('[^A-Za-z0-9]+', ' ', tweet)
    tweet = tweet.lower()
    return tweet

def getSentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return POSITIVE_SENTIMENT
    elif sentiment < 0:
        return NEGATIVE_SENTIMENT
    return NEUTRAL_SENTIMENT

def saveReply(tweet):
    pro_tweet = {}
    idTweet = tweet['in_reply_to_status_id_str']
    pro_tweet['tweetID_reply'] = tweet['id_str']
    pro_tweet['status'] = 0
    if idTweet not in db_rep:
        db_rep[idTweet] = pro_tweet

def saveTweetInDatabase(tweet, number):
    pro_tweet = {}
    #Determines fields to save in json record
    idTweet = tweet['id_str']

    pro_tweet['creation_date'] = tweet['created_at']
    pro_tweet['text'] = cleanTweet(tweet['full_text'])
    pro_tweet['tweet_sentiment'] = getSentiment(pro_tweet['text'])

    pro_tweet['userId'] = tweet['user']['id_str']
    pro_tweet['username'] = tweet['user']['name']
    pro_tweet['screen_name'] = tweet['user']['screen_name']
    pro_tweet['user_location'] = tweet['user']['location']
    pro_tweet['followers_count'] = tweet['user']['followers_count']
    pro_tweet['geo_enabled'] = tweet['user']['geo_enabled']

    pro_tweet['place'] = tweet['place']
    pro_tweet['geo'] = tweet['geo']
    pro_tweet['coordinates'] = tweet['coordinates']

    pro_tweet['topic'] = number

    #Save json record in couchdb
    #if idTweet not in db:
    if idTweet not in db_rep:
        db_rep[idTweet] = pro_tweet

def tweetProcessor(api_interface):
    next_search_id = 0
    last = 0
    topics = setTopics()
    for topic in topics:
        for element in topic:
            query = QUERY_PLACE + element + QUERY_LANGUAGE + QUERY_RETWEET_FLAG
            print(query)
            if element in TOPIC_ONE:
                number = 1
            elif element in TOPIC_TWO:
                number = 2
            else:
                number = 3

            tweets = tweepy.Cursor(api_interface.search, q=query,tweet_mode='extended', exclude_replies=False).items(2000)
            for tweet in tweets:
                print(tweet._json)
                if tweet._json['in_reply_to_status_id'] is not None:
                    saveReply(tweet._json)
                else:
                    saveTweetInDatabase(tweet._json, number)
            time.sleep(180)

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
