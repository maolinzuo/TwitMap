import json
import random
from dateutil import parser
from datetime import datetime
import tweepy

import sys
reload(sys)
sys.setdefaultencoding('UTF8')

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

AWS_ACCESS_KEY=''
AWS_SECRET_KEY=''
region = 'us-east-1' # e.g. us-east-1
service = 'es'

# Variables that contains the user credentials to access Twitter API
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)

host = '' # e.g. my-test-domain.us-east-1.es.amazonaws.com

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def tweets_geo(coordinates):
    filterByGeo = json.dumps({
                    "from" : 0, "size" : 1000,
                    "query" : {
                        "bool" : {
                            "must" : {
                                "match_all" : {}
                            },
                            "filter" : {
                                "geo_distance" : {
                                    "distance" : "100km",
                                    "coordinates" : coordinates
                                }
                            }
                        }
                    }
                })
    return list(map(lambda x: x["_source"], es.search(index="twittmap", doc_type='tweet', body=filterByGeo)['hits']['hits']))

def tweets_key(keyword):
    filterByKeyword = json.dumps({
                       "from" : 0, "size" : 1000,
                       "query": {
                           "match": {
                               'keyword': keyword
                           }
                       },
                       "sort": [
                           {
                               "timestamp": {
                                   "order": "desc"
                               }
                           }
                       ]
                   })
    return list(map(lambda x: x["_source"], es.search(index="twittmap", doc_type='tweet', body=filterByKeyword)['hits']['hits']))

def tweets_all(keyword, coordinates):
    filter = json.dumps({
                       "from" : 0, "size" : 10,
                       "query": {
                           "bool" : {
                               "must" : {
                                   "match" : {
                                        'keyword': keyword
                                   }
                               },
                               "filter" : {
                                   "geo_distance" : {
                                       "distance" : "100km",
                                       "coordinates" : coordinates
                                   }
                               }
                           }
                       },
                       "sort": [
                           {
                               "timestamp": {
                                   "order": "desc"
                               }
                           }
                       ]
                   })
    return list(map(lambda x: x["_source"], es.search(index="twittmap", doc_type='tweet', body=filter)['hits']['hits']))

keySet = ["google", "facebook", "amazon", "apple", "trump", "uber", "movie", "food", "music", "job"]

def getKey(text, keywords):
    results = []
    for keyword in keywords:
        if text.lower().find(keyword) >= 0:
            results.append(keyword)
    return random.choice(results) if results else Exception('no key found')

class TweetStreamListener(tweepy.StreamListener):
    def __init__(self, es):
        self.es = es
        self.rate = 0
        self.other = 0

    def on_data(self, data):
            try:
                tweet = json.loads(data)
                # coordinates: [longitude, latitude]
                document = {'text': tweet['text'],
                            'author': tweet['user']['screen_name'],
                            'keyword': getKey(tweet['text'], keySet),
                            'timestamp': parser.parse(tweet['created_at']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'coordinates': tweet['place']['bounding_box']['coordinates'][0][0] } #  if 'place' in tweet and tweet['place'] else list(random.sample(positions, 1)[::-1])
                # print document['coordinates']
                es.index(index='twittmap', doc_type='tweet', body=document)
                # print document
            except Exception as e:
                # print (e)
                pass

    def on_status(self, status):
        print ("Status: " + status.text)

    def on_error(self, status_code):
        print ('Error:', str(status_code))
        if status_code == 420:
            print ("Rate Limited!")
            sleepy = 60 * math.pow(2, self.rate)
            print (time.strftime("%Y%m%d_%H%M%S"))
            print ("A reconnection attempt will occur in " + \
            str(sleepy/60) + " minutes.")
            time.sleep(sleepy)
            self.rate += 1
        else:
            sleepy = 5 * math.pow(2, self.other)
            print (time.strftime("%Y%m%d_%H%M%S"))
            print ("A reconnection attempt will occur in " + \
            str(sleepy) + " seconds.")
            time.sleep(sleepy)
            self.other += 1
        return False

def tweetStreaming():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth)

    tweetStreamListener = TweetStreamListener(es)
    while True:
        try:
            tweetStream = tweepy.Stream(auth=api.auth, listener=tweetStreamListener)
            tweetStream.filter(track=keySet)
        except KeyboardInterrupt:
            # exit
            tweetStream.disconnect()
            break
        except:
            tweetStream.disconnect()
            continue

if __name__ == '__main__':
    # print tweets_key('job')
    # print map(lambda x: x['coordinates'][0][::-1], tweets_geo([40.899101, -74.720928]))
    # print tweets_key('job')
    print tweets_all('job', [40.899101, -74.720928])
    # tweetStreaming()
