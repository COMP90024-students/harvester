import couchdb
#Local Server
couch = couchdb.Server('http://admin:MGZjZGU5N@45.113.235.78:5984')

#Create databases

couch.create('db_historic')
couch.create('db_streamer')
couch.create('db_replies')
couch.create('db_quoted')
couch.create('db_user')
