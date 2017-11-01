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


awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)


host = 'search-cc6998-ubxdzov3wvjqcmmp7qjc5oc3ny.us-east-1.es.amazonaws.com' # e.g. my-test-domain.us-east-1.es.amazonaws.com

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# search tweets within 500km of the clicked point
def tweets_geo(coordinates):
    filterByGeo = json.dumps({
                    "from" : 0, "size" : 500,
                    "query" : {
                        "bool" : {
                            "must" : {
                                "match_all" : {}
                            },
                            "filter" : {
                                "geo_distance" : {
                                    "distance" : "500km",
                                    "coordinates" : coordinates
                                }
                            }
                        }
                    }
                })
    return list(map(lambda x: x["_source"], es.search(index="twittmap", doc_type='tweet', body=filterByGeo)['hits']['hits']["_source"]))

# 500 tweets without any requirment
def tweets_init():
    filterall = json.dumps({
                       "from" : 0, "size" : 500,
                       "query": {
                            "match_all": {}
                        }
                   })
    return list(map(lambda x: x["_source"],es.search(index="twittmap", doc_type='tweet', body=filterall)['hits']['hits'][1:]["_source"]))

# search tweets according to keywords
def tweets_key(keyword):
    filterByKeyword = json.dumps({
                       "from" : 0, "size" : 500,
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
    return list(map(lambda x: x["_source"],es.search(index="twittmap", doc_type='tweet', body=filterByKeyword)['hits']['hits']["_source"]))

# search tweets according to both keywords and coordinates
def tweets_all(keyword, coordinates):
    filter = json.dumps({
                       "from" : 0, "size" : 500,
                       "query": {
                           "bool" : {
                               "must" : {
                                   "match" : {
                                        'keyword': keyword
                                   }
                               },
                               "filter" : {
                                   "geo_distance" : {
                                       "distance" : "500km",
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
    return list(map(lambda x: x["_source"],es.search(index="twittmap", doc_type='tweet', body=filter)['hits']['hits']["_source"]))

keySet = ["google", "facebook", "amazon", "apple", "trump", "uber", "movie", "food", "music", "job"]


if __name__ == '__main__':
    # print tweets_key('job')
    # print map(lambda x: x['coordinates'][0][::-1], tweets_geo([40.899101, -74.720928]))
    # print tweets_key('job')
    print tweets_all('job', [40.899101, -74.720928])
    # tweetStreaming()
