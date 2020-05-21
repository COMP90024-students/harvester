import datetime
import re

import couchdb
from geopy.geocoders import Nominatim
from textblob import TextBlob

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


POSITIVE_SENTIMENT = 1
NEGATIVE_SENTIMENT = -1
NEUTRAL_SENTIMENT = 0

MONTH = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}

#stop_words = list(set(stopwords.words('english')))

#Local Server
couch = couchdb.Server('http://admin:Cad1020*@127.0.0.1:5984')
global db_hist
db_hist = couch['db_historic']

global db_raw
db_raw = couch['db_rawproc']

global geolocator
geolocator = Nominatim(user_agent="my-application")

def getMainText(tweet):
    try:
        text = tweet['full_text']
    except:
        text = tweet['text']
    return text


def getLocation(tweet):
    loc = {}
    if tweet['geo'] is not None:
        location = tweet['geo']
        coordinates = location['coordinates']
        location = geolocator.reverse(str(coordinates[0]) + ',' + str(coordinates[1]))
        location = geolocator.geocode(location.address)
        loc['lat'] = location.latitude
        loc['lon'] = location.longitude
        location = location.address.split()
        loc['suburb'] = location[1]
        loc['city'] = location[2]
        loc['state'] = location[4]
        loc['country'] = location[5]
        return loc
    elif tweet['place'] is not None:
        if tweet['place']['place_type'] in ['city', 'poi']:
            name = tweet['place']['full_name'].split()
            location = geolocator.geocode(name)
            loc['lat'] = location.latitude
            loc['lon'] = location.longitude
            location = location.address.split(', ')
            if len(location) == 3:
                loc['city'] = location[0]
                loc['state'] = location[1]
                loc['country'] = location[2]
            elif len(location) == 4:
                loc['city'] = location[0]
                loc['state'] = location[1]
                loc['country'] = location[3]
            elif len(location) == 5:
                loc['city'] = location[0]
                loc['state'] = location[2]
                loc['country'] = location[4]
        if tweet['place']['place_type'] in ['admin']:
            name = tweet['place']['full_name'].split()
            location = geolocator.geocode(name)
            loc['lat'] = location.latitude
            loc['lon'] = location.longitude
            location = location.address.split(', ')
            loc['state'] = location[0]
            loc['country'] = location[1]
        return loc
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
    return date.month, date.year


def tweetProcessor():
    for row in db_hist:
        if row in db_raw:
            id = db_raw[row]
            tweet = db_hist[row]
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
            db_raw.delete(id)
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
16728439
ssh cdavaloscast@spartan.hpc.unimelb.edu.au
neskyz-8Dyfva-qytsah

source /usr/local/module/spartan_new.sh
module load gcc/8.3.0
module load python/3.7.4
module load openmpi/3.1.4
pip install geopy --user
pip install couchdb --user

cd Assignment2
python tweetsFromReplies.py
python currentTweets.py

mpirun -n 8 python processParallelTweets.py

sbatch 1N_8C_Process.slurm

curl "http://45.113.232.90/couchdbro/twitter/_design/twitter/_view/summary" \
-G \
--data-urlencode 'start_key=["melbourne",2014,1,1]' \
--data-urlencode 'end_key=["melbourne",2019,12,31]' \
--data-urlencode 'reduce=false' \
--data-urlencode 'include_docs=true' \
--user "readonly:ween7ighai9gahR6" \
-o /Volumes/Transcend/twitter.json



mpirun -n 4 python processParallelTweets.py


 cd processTweets



ssh -i "/Users/carlosandresdavalos/Downloads/comp90024-group19.pem" ubuntu@115.146.92.83


sudo ssh -i "/Users/carlosandresdavalos/Downloads/comp90024-group19.pem" ubuntu@115.146.92.83


811500


./.local/lib/python3.7/site-packages
