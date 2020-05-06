#------------------------------------
import json
import numpy as np 
import itertools
import collections
import string
import re
import numpy as np
import pandas as pd

#------------------------------------
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()
#------------------------------------------------------------------



def sentiment_analyzer_scores(text):
    score = analyser.polarity_scores(text)
    lb = score['compound']
    polarity_score=0
    if lb >= 0.05:
        polarity_score=1
    elif (lb > -0.05) and (lb < 0.05):
        polarity_score=0
    else:
        polarity_score=-1
      
    return  polarity_score,lb


def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)        
    return input_txt
  
  
def clean_tweets(lst):
      # remove twitter Return handles (RT @xxx:)
    lst = remove_pattern(lst, "RT @[\w]*:")
      # remove twitter handles (@xxx)
    lst = remove_pattern(lst, "@[\w]*")
      # remove URL links (httpxxx)
    lst = remove_pattern(lst, "https?://[A-Za-z0-9./]*")
      # remove special characters, numbers, punctuations (except for #)
    lst = lst.replace("[^a-zA-Z#]", " ")
    return lst



def tweet_sentiment(text):
    #cleaning tweet
    tweet_text=clean_tweets(text)
    #get the ploarity degree and its percentage
    polarity_score,polarity_percentage=sentiment_analyzer_scores(tweet_text)
    
    return polarity_score,polarity_percentage
        
  
#example
tweet="I’m still not sure about that new blue Big Ben colour scheme. Even if it is the long-lost original one"
print(tweet_sentiment(tweet))


