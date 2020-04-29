import couchdb
#Local Server
couch = couchdb.Server('http://admin:Cad17181046@127.0.0.1:5984')

#Remote Server
#couch = couchdb.Server('server name ')
#Si tiene usuario y contrasenia
#couch = couchdb.Server('username:password@server name ')

#Create database
db = couch.create('db_test')
#If using existing one

#db = couch['db_test']

#print(list(db.view('tweet/tweetID-view')['key']))

'''
db['100'] = {'hola': 'hola'}

#Create new instance
doc = {
    'id': 123456789
}
db.save(doc)

UserDb_Design = 'user'
UserDb_View = 'new-view'
print(db.view(UserDb_Design+'/'+UserDb_View))

UserDb_Design = 'user'
UserDb_View = 'userID-view'
for row in  db.view(UserDb_Design+'/'+UserDb_View):
    print(row['key'])
'''

'''
install Couchdb

create database using python and couch.create('db_test')
- Create user: https://docs.couchdb.org/en/stable/intro/security.html
HOST="http://admin:password@127.0.0.1:5984"
NODENAME="_local"
curl -X PUT $HOST/_node/$NODENAME/_config/admins/username -d '"password"'
from terminal we can access the couchdb
- install brew 
- install brew install curl
https://docs.couchdb.org/en/stable/intro/curl.html
- curl http://admin:Cad17181046@127.0.0.1:5984
- all databases in couchdb: curl -X GET http://admin:Cad17181046@127.0.0.1:5984/_all_dbs
- Para crear un view toca ir a cd /usr/local/opt/curl/bin
- crear archivo json con las indicaciones

{
  "_id": "_design/nombredocumento",
  "_rev": "5-11800513f56d5ee20c3bc1e7a170cf53",
  "views": {
    "nombreview-view": {
      "map": "function (doc) {\n  emit(doc._id, 1);\n}"
    }
  },
  "language": "javascript"
}

- correr curl -X PUT http://admin:Cad17181046@127.0.0.1:5984/db_test/_design/example --data-binary @java.json

'''



