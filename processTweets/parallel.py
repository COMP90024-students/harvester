# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
# Import csv to read files and arguments
import csv, sys, getopt
# Import regular expressions to look for topics and mentions, json to parse tweet data
import re, json, ijson, operator
from collections import defaultdict
import time
import couchdb
from textblob import TextBlob
import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


# Define the constants
MASTER_RANK = 0
global db_hist
global db_test
global db_raw


STATES = ['New South Wales', 'Victoria', 'Queensland', 'Western Australia', 'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory']

TOPIC_ONE = ['covid app', 'covidsafe', 'safe app', 'safeapp', 'tracing app', 'tracking app', 'covid safe app', 'coronavirus app', 'covid19 app']
TOPIC_TWO = ['scottmorrisonmp', 'scomo', 'morrison', 'government', 'gov', 'jobseeker', 'jobkeeper', 'unemployment',
             'minister', 'parliament', 'corruption', 'pm', 'premier', 'victorians', 'govt', 'danielandrewsmp', 'scotty',
             'auspol', ' greghuntmp', 'auswake', 'peterdutton', 'economy', 'lockdown', 'politicians', 'politics',
             'andrews', 'danandrews', 'political', 'goverments', 'health', 'school', 'schools', 'dan tehan', 'jennymikakos', 'mikakos',
             'governors', 'governor', 'teachers', 'dantehanwannon', 'servicesgovau', 'job seeker', 'job keeper']

global geolocator
geolocator = Nominatim(user_agent="my-application", timeout=None)
#geolocator = RateLimiter(geolocator.geocode, min_delay_seconds=1)

POSITIVE_SENTIMENT = 1
NEGATIVE_SENTIMENT = -1
NEUTRAL_SENTIMENT = 0

MONTH = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}


def db_connection():
  couch = couchdb.Server('http://admin:Cad1020*@127.0.0.1:5984')
  db_hist = couch['db_historic']
  db_test = couch['db_testrep']
  db_raw = couch['db_rawproc']
  return db_hist,db_test,db_raw

def getMainText(tweet):
    try:
        text = tweet['full_text']
    except:
        text = tweet['text']
    return text

def reverse_geo(coordinates):
    rev_location = geolocator.reverse(coordinates, timeout=None)
    return rev_location

def getLocation(tweet):
    loc = {}
    if tweet['geo'] is not None:
        print(tweet['id_str'])
        location = tweet['geo']
        coordinates = location['coordinates']
        aprox_loc = reverse_geo(coordinates)
        if aprox_loc is not None:
            address = aprox_loc.address.split(', ')
            if address[len(address) - 1] == 'Australia':
                loc['lat'] = round(aprox_loc.latitude,3)
                loc['lon'] = round(aprox_loc.longitude,3)
                if len(address) == 6:
                    loc['suburb'] = address[1]
                    loc['city'] = address[2]
                    loc['state'] = address[3]
                    loc['country'] = address[len(address) - 1]
                    return loc
    elif tweet['place'] is not None and tweet['place']['country'] == 'Australia':
        print(tweet['id_str'])
        if tweet['place']['place_type'] in ['neighborhood']:
            name = tweet['place']['full_name']
            location = geolocator.geocode(name, timeout=None)
            if location is None:
                name = tweet['place']['name'] + ', ' + tweet['place']['country']
                location = geolocator.geocode(name, timeout=None)
            if location is None:
                loc = None
            else:
                loc['lat'] = round(location.latitude,3)
                loc['lon'] = round(location.longitude,3)
                location = location.address.split(', ')
                if len(location) == 2:
                    loc['city'] = location[0]
                    loc['state'] = tweet['place']['full_name'].split(', ')[1]
                    loc['country'] = location[1]
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
                elif len(location) == 6:
                    loc['sururb'] = location[1]
                    loc['city'] = location[2]
                    loc['state'] = location[3]
                    loc['country'] = location[5]
                elif len(location) == 7:
                    loc['suburb'] = location[2]
                    loc['city'] = location[3]
                    loc['state'] = location[4]
                    loc['country'] = location[6]
                elif len(location) == 8:
                    loc['suburb'] = location[2]
                    loc['city'] = location[3]
                    loc['state'] = location[5]
                    loc['country'] = location[7]
        elif tweet['place']['place_type'] in ['city']:
            name = tweet['place']['full_name']
            location = geolocator.geocode(name, timeout=None)
            if location is None:
                name = tweet['place']['name'] + ', ' + tweet['place']['country']
                location = geolocator.geocode(name, timeout=None)
            if location is None:
                loc = None
            else:
                loc['lat'] = round(location.latitude,3)
                loc['lon'] = round(location.longitude,3)
                location = location.address.split(', ')
                if len(location) == 2:
                    loc['city'] = location[0]
                    loc['state'] = tweet['place']['full_name'].split(', ')[1]
                    loc['country'] = location[1]
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
                elif len(location) == 6:
                    loc['sururb'] = location[1]
                    loc['city'] = location[2]
                    loc['state'] = location[3]
                    loc['country'] = location[5]
                elif len(location) == 7:
                    loc['suburb'] = location[1]
                    loc['city'] = location[2]
                    loc['state'] = location[4]
                    loc['country'] = location[6]
                elif len(location) == 8:
                    loc['suburb'] = location[2]
                    loc['city'] = location[3]
                    loc['state'] = location[5]
                    loc['country'] = location[7]
        elif tweet['place']['place_type'] in ['poi']:
            name = tweet['place']['full_name'] + ', ' + tweet['place']['country']
            location = geolocator.geocode(name, timeout=None)
            if location is None:
                name = tweet['place']['country']
                location = geolocator.geocode(name, timeout=None)
            if location is not None:
                loc['lat'] = round(location.latitude,3)
                loc['lon'] = round(location.longitude,3)
                location = location.address.split(', ')
                print(location)
                if len(location) == 1:
                    loc['country'] = location[0]
                elif len(location) == 2:
                    loc['state'] = location[0]
                    loc['country'] = location[1]
                elif len(location) == 3:
                    loc['state'] = location[1]
                    loc['country'] = location[2]
                elif len(location) == 4:
                    loc['state'] = location[1]
                    loc['country'] = location[3]
                elif len(location) == 5:
                    loc['city'] = location[1]
                    loc['state'] = location[2]
                    loc['country'] = location[4]
                elif len(location) == 6:
                    loc['suburb'] = location[1]
                    loc['city'] = location[2]
                    loc['state'] = location[3]
                    loc['country'] = location[5]
                elif len(location) == 7:
                    loc['city'] = location[2]
                    loc['state'] = location[4]
                    loc['country'] = location[6]
                elif len(location) == 8:
                    loc['suburb'] = location[2]
                    loc['city'] = location[4]
                    loc['state'] = location[6]
                    loc['country'] = location[7]
                elif len(location) == 9:
                    loc['suburb'] = location[2]
                    loc['city'] = location[5]
                    loc['state'] = location[6]
                    loc['country'] = location[8]
        elif tweet['place']['place_type'] in ['admin']:
            name = tweet['place']['full_name']
            location = geolocator.geocode(name, timeout=None)
            loc['lat'] = round(location.latitude,3)
            loc['lon'] = round(location.longitude,3)
            location = location.address.split(', ')
            loc['state'] = location[0]
            loc['country'] = location[1]
        elif tweet['place']['place_type'] in ['country']:
            name = tweet['place']['full_name']
            location = geolocator.geocode(name, timeout=None)
            loc['lat'] = round(location.latitude,3)
            loc['lon'] = round(location.longitude,3)
            location = location.address.split(', ')
            loc['country'] = location[0]
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

def tweetProcessor(tweet):
  data = {}
  data['location'] = getLocation(tweet)
  if data['location'] is None:
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
  data['topic'] = filterTopic(text_topic)
  data['sentiment'] = getSentiment(cleanTweetSentiment(text))
  data['month'],data['year'] = getDate(tweet)
  if (data['location'] is not None) and (data['topic'] > -1) :
      print(data['location'])
      if data['location']['country'] == 'Australia' :
          return data
  return None

def process_json_tweets(rank, processes = 3):
  # Open the json file containing all the tweets
  try:
    db_hist,db_test,db_raw = db_connection()
    for i, line in enumerate(db_raw):
      if i%processes == rank:
        try:
          # Count frequency of hashtags
          print(line)
          id = db_raw[line]
          tweet = db_hist[line]
          pro_tweet = tweetProcessor(tweet)
          if line not in db_test and pro_tweet is not None:
            db_test[str(line)] = pro_tweet
          db_raw.delete(id)
        except ValueError:
          print("Malformed JSON in tweet", i)
        except:
          print("Unexpected error:", sys.exc_info()[0])
          raise
  except TypeError:
    print("Could not read line in json.")

def master_data_processor(comm):
    # Get rank and size
    rank = comm.Get_rank()
    size = comm.Get_size()
    process_json_tweets(rank, size)

def slave_data_processor(comm):
  # Get the current processor rank and size
  rank = comm.Get_rank()
  size = comm.Get_size()

  process_json_tweets(rank)
  # Wait for a communication from master processor to send the processed data
  while True:
    in_comm = comm.recv(source=MASTER_RANK, tag=rank)


def main():
  # Identify whether the current node is a master processor or slave processor
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()

  print("This message is sent from rank %s." % (rank))
  if rank == 0 :
    # Rank 0 indicates that the current processor a master processor
    master_data_processor(comm)
  else:
    # Rank>0 indicates a current processor is a slave processor
    slave_data_processor(comm)

# This method gets invoked first when executed from slurm job
if __name__ == "__main__":
    main()
