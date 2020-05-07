import couchdb
#Local Server
couch = couchdb.Server('http://admin/admin@115.146.95.10:5984')

#Create databases
couch.create('db_historic')
couch.create('db_streamer')
couch.create('db_replies')
couch.create('db_quoted')
couch.create('db_user')
