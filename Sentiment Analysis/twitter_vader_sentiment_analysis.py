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
    if lb >= 0.05:
        return 1
    elif (lb > -0.05) and (lb < 0.05):
        return 0
    else:
        return -1


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




def twitter_dict(file_name) :

   
    
	tweetsList=list()
	sentiments_scores=list()
	
	file =file_name 
	
    
	with open(file, 'r', encoding='utf-8',errors='ignore') as load:
		for tweets in load:
			 try:
					if(tweets[-2:-1]==','):
						 twitter_data=json.loads(tweets[:-2])
					else:
						twitter_data=json.loads(tweets[:-1])
			 except Exception:
					continue
                  
			 tweet_=twitter_data['doc']['text']
			 
			 tweet_text=clean_tweets(tweet_)
			 tweetsList.append(tweet_text)
			 sentiment_score=sentiment_analyzer_scores(tweet_text)
			
			 sentiments_scores.append(sentiment_score)
            
	return tweetsList,sentiments_scores         
             
             
			 
             
tweets,sentiments=twitter_dict("tinyTwitter.json") 

df = pd.DataFrame(tweets,columns = ["Text"])
df["Sentiment"]=sentiments


df.to_csv (r'twitter_sentiment_analysis_results.csv', index = False, header=True)
