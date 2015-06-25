 # Team: 6
 # City: Dallas
 # Members:
 #    Aksheta Mehta (620522)  
 #    Arun Hariharan Sivasubramaniyan (662528)
 #    Bruno de Assis Marques (659338)
 #    Jordan Burdett (392025)
 #    Yanyi Liang (642967)

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
from textblob import TextBlob
import sys
import couchdb

city = "Dallas"
dbname = "dallas"

# Getting initial arguments for instance id, consume and access tokens and secrets and database url
idd = int(sys.argv[1])%4
consumer_key =  sys.argv[2]
consumer_secret = sys.argv[3]
access_token = sys.argv[4]
access_token_secret = sys.argv[5]
database_url = sys.argv[6]

class StdOutListener(StreamListener):
    def on_data(self, data):
      self.analyse(data)
      return True

    def on_error(self, status):
      print(status)

    def analyse(self, data):        
      tweet = json.loads(data)
      polarity = 0
      tweet['text'] = tweet['text'].encode('ascii', 'ignore')
      
      if tweet['text'].startswith("RT "):
        return

      blob = TextBlob(tweet['text'])
      numsentences = 0
      for sentence in blob.sentences:
        polarity += sentence.sentiment.polarity
        numsentences +=1
      if (numsentences != 0):
        polarity /= numsentences
      record = {}
      record['tweet'] = tweet
      record['polarity'] = polarity
      text = tweet['text'].lower()
      record['obama'] = (text.find('barack') != -1) or (text.find('obama') != -1)
      record['harvester'] = idd
      record['_id'] = str(tweet['id'])
      try:
        db.save(record)
      except couchdb.http.ResourceConflict, err:
        pass

if __name__ == '__main__':
  # Connects to the database and create dallas database if it does not exist
  couch = couchdb.Server(database_url)
  try:
    db = couch[dbname]
  except couchdb.http.ResourceNotFound, err:
    db = couch.create(dbname)

  if not db:
    db = couch.create(dbname)

  l = StdOutListener()
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)

  api = tweepy.API(auth)

  # Gets the place polygon
  places = api.geo_search(query=city, granularity="city")
  stream = Stream(auth, l)

  x1 = places[0].bounding_box.coordinates[0][0][0]
  y1 = places[0].bounding_box.coordinates[0][0][1]
  x2 = places[0].bounding_box.coordinates[0][2][0]
  y2 = places[0].bounding_box.coordinates[0][2][1]

  # Starts listening to the stream for that area
  stream.filter(locations = [x1,y1,x2,y2])