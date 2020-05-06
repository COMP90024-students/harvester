import couchdb

couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')
db_stream = couch['db_streamer']
db_hist = couch['db_historic']
db_user = couch['db_user']

for row in db_stream:
    tweet = db_stream[row]
    id_user = tweet['user']['id_str']
    if id_user not in db_user:
        pro_tweet = {}
        pro_tweet['flag'] = 0
        db_user[id_user] = pro_tweet

for row in db_hist:
    tweet = db_hist[row]
    id_user = tweet['user']['id_str']
    if id_user not in db_user:
        pro_tweet = {}
        pro_tweet['flag'] = 0
        db_user[id_user] = pro_tweet
