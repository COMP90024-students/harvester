import couchdb
import re
from textblob import TextBlob
import datetime
from geopy.geocoders import Nominatim
'''
from nltk.tokenize import *
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
'''

TOPIC_ONE = ['covid app', 'covidsafe', 'safe app', 'safeapp', 'tracing app', 'tracking app', 'covid safe app', 'coronavirus app', 'covid19 app']
TOPIC_TWO = ['scottmorrisonmp', 'scomo', 'morrison', 'government', 'gov', 'jobseeker', 'jobkeeper', 'unemployment',
             'minister', 'parliament', 'corruption', 'pm', 'premier', 'victorians', 'govt', 'danielandrewsmp', 'scotty',
             'auspol', ' greghuntmp', 'auswake', 'peterdutton', 'economy', 'lockdown', 'politicians', 'politics',
             'andrews', 'danandrews', 'political', 'goverments', 'health', 'school', 'schools', 'dan tehan', 'jennymikakos', 'mikakos',
             'governors', 'governor', 'teachers', 'dantehanwannon', 'servicesgovau', 'job seeker', 'job keeper']

global geolocator
geolocator = Nominatim()

POSITIVE_SENTIMENT = 1
NEGATIVE_SENTIMENT = -1
NEUTRAL_SENTIMENT = 0

MONTH = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}

#stop_words = list(set(stopwords.words('english')))

#Local Server
couch = couchdb.Server('http://admin:Cad1020*@127.0.0.1:5984')
global db_hist
db_hist = couch['db_historic']

def getMainText(tweet):
    try:
        text = tweet['full_text']
    except:
        text = tweet['text']
    return text


def getLocation(tweet):
    if tweet['geo'] is not None:
        location = tweet['geo']
        coordinates = location['coordinates']
        return location
    elif tweet['place'] is not None:
        location = {'place_type': tweet['place']['place_type'], 'name': tweet['place']['full_name'],
                    'full_name': tweet['place']['bounding_box']['coordinates']}
        return location
    return None


def filterTopic(text):
    for word in text.split():
        if word in TOPIC_ONE and word not in TOPIC_TWO:
            return 1
        elif word not in TOPIC_ONE and word in TOPIC_TWO:
            return 2
        elif word in TOPIC_ONE and word in TOPIC_TWO:
            return 3
    return 0


def cleanTweet(tweet):
    tweet = re.sub(r"http\S+", '', tweet, flags=re.MULTILINE)
    tweet = re.sub('[@]|#|RT|!|[.]|:', '', tweet)
    tweet = re.sub('\n', ' ', tweet)
    tweet = re.sub('  ', ' ', tweet)
    tweet = tweet.lower()
    return tweet


def cleanTweetSentiment(tweet):
    tweet = re.sub(r"http\S+|@[A-Za-z0-9]+", '', tweet, flags=re.MULTILINE)
    tweet = re.sub('[@]|#|RT|[.]|:', '', tweet)
    tweet = re.sub('\n', ' ', tweet)
    tweet = re.sub('  ', ' ', tweet)
    return tweet


def getSentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return POSITIVE_SENTIMENT
    elif sentiment < 0:
        return NEGATIVE_SENTIMENT
    return NEUTRAL_SENTIMENT


def getDate(tweet):
    date = tweet['created_at'].split()
    date = datetime.datetime(int(date[5]), MONTH[date[1]], int(date[2]))
    return date


def tweetProcessor():
    for row in db_hist:
        tweet = db_hist['1009324695504801792']
        location = getLocation(tweet)
        if location is None:
            next
        text = getMainText(tweet)
        try:
            retweet = tweet['retweeted_status']['text']
            text = retweet + text
        except:
            next
        try:
            quote = tweet['tweet_quoted_to']
            text = quote + text
        except:
            next
        try:
            reply = tweet['tweet_replied_to']
            text = reply + text
        except:
            next
        text_topic = cleanTweet(text)
        topic = filterTopic(text_topic)
        if topic:
            next
        sentiment = getSentiment(cleanTweetSentiment(text))
        if (location is not None) and (topic > 0):
            print(row, getDate(tweet), location, topic, sentiment)
    #sid = SentimentIntensityAnalyzer()
    #ss = sid.polarity_scores(textSentiment)
    #print(ss)
    # tokenize
    #tokenized = word_tokenize(textSentiment)

    # remove stopwords
    #stopped = [w for w in tokenized if not w in stop_words]
    #print(stopped)
    '''
    for row in db_hist:
        tweet = db_hist['1000019550606262272']
        text = getMainText(tweet)
        try:
            retweet = tweet['retweeted_status']['text']
            text = retweet + text
        except:
            continue
        try:
            quote = tweet['tweet_quoted_to']
            text = quote + text
        except:
            continue
        try:
            reply = tweet['tweet_replied_to']
            text = reply + text
        except:
            continue

        textSentiment = cleanTweetSentiment(text)
        print(textSentiment)
        break
    '''
if __name__ == '__main__':
    tweetProcessor()
