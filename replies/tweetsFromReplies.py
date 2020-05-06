import tweepy
import time
import couchdb
import re
from textblob import TextBlob

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

TOPIC_ONE = ['covid app', 'covidsafe', 'safe app', 'safeapp', 'tracing app', 'tracking app', 'covid safe app', 'coronavirus app', 'covid19 app']
TOPIC_TWO = ['scottmorrisonmp', 'scomo', 'morrison', 'government', 'gov', 'jobseeker', 'jobkeeper', 'unemployment',
             'minister', 'parliament', 'corruption', 'pm', 'premier', 'victorians', 'govt', 'danielandrewsmp', 'scotty',
             'auspol', 'greghuntmp', 'auswake', 'peterdutton', 'economy', 'lockdown', 'politicians', 'politics',
             'andrews', 'danandrews', 'political', 'goverments', 'health', 'school', 'schools', 'dan tehan', 'jennymikakos', 'mikakos',
             'governors', 'governor', 'teachers', 'dantehanwannon']

def setTopics():
    QUERY_TOPIC = []
    QUERY_TOPIC.append(TOPIC_ONE)
    QUERY_TOPIC.append(TOPIC_TWO)
    #QUERY_TOPIC.append(TOPIC_THREE)
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
    if idTweet not in db:
        db[idTweet] = pro_tweet

def tweetProcessor(api_interface):
    topics = setTopics()
    for row in db_rep:
        doc = db_rep[row]
        print(row)
        if doc['status'] <= 1:
            for topic in topics:
                if topic == TOPIC_ONE:
                    number = 1
                elif topic == TOPIC_TWO:
                    number = 2
                else:
                    number = 3
                try:
                    statusOne = api_interface.get_status(row, tweet_mode="extended")
                    tweet = cleanTweet(statusOne._json['full_text'])
                    tweet = tweet.split()
                    for element in tweet:
                        if element in topic:
                            saveTweetInDatabase(statusOne._json, number)
                            print(row)
                            statusTwo = api_interface.get_status(db_rep[row]['tweetID_reply'], tweet_mode="extended")
                            print(statusOne._json['full_text'] + " " +  statusTwo._json['full_text'])
                            statusTwo._json['full_text'] = statusOne._json['full_text'] + " " +  statusTwo._json['full_text']
                            saveTweetInDatabase(statusTwo._json, number)
                            print('good')
                except:
                    next
        doc['status'] = 2
        db_rep.save(doc)

def harvestTweets():
    api_interface = setCredentials()
    tweetProcessor(api_interface)

if __name__ == '__main__':
    harvestTweets()
