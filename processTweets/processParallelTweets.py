# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
# Import csv to read files and arguments
import sys
# Import regular expressions to look for topics and mentions, json to parse tweet data
import re
import couchdb
import datetime
from geopy.geocoders import Nominatim
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# Define the constants
MASTER_RANK = 0
global db_hist
global db_ui

TOPIC_ONE = ['covid app', 'covidsafe', 'safe app', 'safeapp', 'tracing app', 'tracking app', 'covid safe app', 'coronavirus app', 'covid19 app']
TOPIC_TWO = ['scottmorrisonmp', 'scomo', 'morrison', 'government', 'gov', 'jobseeker', 'jobkeeper', 'unemployment',
             'minister', 'parliament', 'corruption', 'pm', 'premier', 'victorians', 'govt', 'danielandrewsmp', 'scotty',
             'auspol', ' greghuntmp', 'auswake', 'peterdutton', 'economy', 'lockdown', 'politicians', 'politics',
             'andrews', 'danandrews', 'political', 'goverments', 'health', 'school', 'schools', 'dan tehan', 'jennymikakos', 'mikakos',
             'governors', 'governor', 'teachers', 'dantehanwannon', 'servicesgovau', 'job seeker', 'job keeper', 'michael mccormack',
             'm_mccormackmp', 'josh frydenberg', 'joshfrydenberg', 'bridget mckenzie', 'senbmckenzie',
             'mathias cormann', 'mathiascormann', 'simon birmingham', 'birmo', 'christian porter',
             'cporterwa', 'marise payne', 'marisepayne', 'peter dutton', 'peterdutton_mp', 'linda reynolds',
             'lindareynoldswa', 'alan tudge', 'alantudgemp', 'paul fletcher', 'paulfletchermp', 'dan tehan',
             'dantehanwannon', 'michaelia cash', 'senatorcash', 'matt canavan', 'mattjcan', 'angus taylor',
             'angustaylormp', 'sussan ley', 'sussanley', 'anne ruston', 'anne_ruston', 'ken wyatt', 'kenwyattmp',
             'david littleproud', 'd_littleproudmp', 'stuart robert', 'stuartrobertmp', 'paul de jersey', 'qldgovernor',
             'hieu van le', 'hieu van le', 'southaustralia', 'kate warner', 'vicgovernor', 'linda dessau',
             'annastacia palaszczuk', 'annastaciamp', 'gladys berejiklian', 'gladysb', 'mark mcgowan',
             'markmcgowanmp', 'steven marshall', 'peter gutwein']

global geolocator
geolocator = Nominatim(user_agent="my-application", timeout=None)

analyser = SentimentIntensityAnalyzer()

POSITIVE_SENTIMENT = 1
NEGATIVE_SENTIMENT = -1
NEUTRAL_SENTIMENT = 0

MONTH = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}


def db_connection():
    couch = couchdb.Server('http://admin:MGZjZGU5N@45.113.235.78:5984')
    db_hist = couch['db_historic']
    db_ui = couch['ui_db']
    return db_hist,db_ui

def getMainText(tweet):
    try:
        text = tweet['full_text']
    except:
        text = tweet['text']
        pass
    return text

def get_address(location):
    address = location.raw['address']
    loc = {}
    loc['lat'] = round(location.latitude,3)
    loc['lon'] = round(location.longitude,3)
    try:
        loc['suburb'] = address['suburb']
    except:
        next
    try:
        loc['city'] = address['city']
    except:
        next
    try:
        loc['county'] = address['county']
    except:
        next
    try:
        loc['state'] = address['state']
    except:
        next
    try:
        loc['country'] = address['country']
    except:
        next
    try:
        loc['postcode'] = address['postcode']
    except:
        next

    return loc


def getLocation(tweet):
    loc = {}
    if tweet['geo'] is not None:
        location = tweet['geo']
        coordinates = location['coordinates']
        location = geolocator.reverse(coordinates,addressdetails=True)
        if location is None:
            loc = None
        else:
            loc = get_address(location)
        return loc
    elif tweet['place'] is not None and tweet['place']['country'] == 'Australia':
        name = tweet['place']['full_name']
        location = geolocator.geocode(name, country_codes='AU',addressdetails=True)
        if location is None:
            name = tweet['place']['name'] + ', ' + tweet['place']['country']
            location = geolocator.geocode(name, country_codes='AU',addressdetails=True)
        if location is None:
            loc = None
        else:
            loc = get_address(location)
    elif tweet['user']['location'] is not None :
        name = tweet['user']['location']
        location = geolocator.geocode(name, country_codes='AU',addressdetails=True)
        if location is None:
            loc = None
        else:
            loc = get_address(location)
        return loc
    else:
        loc = None
    return loc


def filterTopic(text):
    flag_one = 0
    flag_two = 0
    for word in text.split():
        if word in TOPIC_ONE and word not in TOPIC_TWO:
            flag_one = 1
        elif word not in TOPIC_ONE and word in TOPIC_TWO:
            flag_two = 1
    if flag_one == 1 and flag_two == 1:
        return 3
    elif flag_one == 1 and flag_two == 0:
        return 1
    elif flag_one == 0 and flag_two == 1:
        return 2
    return 0


def cleanTweet(tweet):
    tweet = re.sub(r"https?://[A-Za-z0-9./]*", '', tweet, flags=re.MULTILINE)
    tweet = re.sub('[@]|#|RT|[.]|:', '', tweet)
    tweet = re.sub('\n', ' ', tweet)
    tweet = re.sub('  ', ' ', tweet)
    tweet = tweet.lower()
    return tweet


def cleanTweetSentiment(tweet):
    tweet = re.sub(r"https?://[A-Za-z0-9./]*", '', tweet, flags=re.MULTILINE)
    tweet = re.sub('[@]|#|RT|[.]|:', '', tweet)
    tweet = re.sub('\n', ' ', tweet)
    tweet = re.sub('  ', ' ', tweet)
    return tweet


def sentiment_analyzer_scores(text):
    score = analyser.polarity_scores(text)
    lb = score['compound']
    polarity_score=0
    if lb >= 0.05:
        polarity_score=POSITIVE_SENTIMENT
    elif (lb > -0.05) and (lb < 0.05):
        polarity_score=NEUTRAL_SENTIMENT
    else:
        polarity_score=NEGATIVE_SENTIMENT

    return  polarity_score,lb


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
  polarity, sentiment = sentiment_analyzer_scores(cleanTweetSentiment(text))
  data['polarity'] = polarity
  data['sentiment'] = sentiment
  data['month'],data['year'] = getDate(tweet)
  if (data['location'] is not None) and (data['topic'] > -1) :
      if data['location']['country'] == 'Australia' :
          return data
  return None

#520000
def process_json_tweets(rank, processes = 8):
    db_hist,db_ui = db_connection()
    for i, line in enumerate(db_hist):
        if i%processes == rank:
            print(i)
            tweet = db_hist[line]
            #try:
            #    flag = tweet['flag']
            #    print(flag)
            #except:
            if tweet['flag'] == 0:
                pro_tweet = tweetProcessor(tweet)
                if line not in db_ui and pro_tweet is not None:
                    print(i, line)
                    db_ui[str(line)] = pro_tweet
                    tweet['flag'] = 1
                    db_hist.save(tweet)

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
