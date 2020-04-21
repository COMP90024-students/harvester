import couchdb
#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')

#Remote Server
#couch = couchdb.Server('server name ')
#Si tiene usuario y contrasenia
#couch = couchdb.Server('username:password@server name ')

#Create database
#db = couch.create('db_test')
#If using existing one
db = couch['db_test']

#Create new instance
doc = {
    'id': 123456789
}
db.save(doc)




