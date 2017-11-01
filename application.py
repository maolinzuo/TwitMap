import json
from threading import Thread, Event
from flask import Flask, render_template, request
from flask.ext.socketio import SocketIO, emit
from tweetUtil import *

flag = Event()
application = Flask(__name__)
socketio = SocketIO(application)

# Variables that contains the user credentials to access Twitter API
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

class TweetStreamListener(tweepy.StreamListener):
    def __init__(self, es):
        self.es = es
        self.rate = 0
        self.other = 0

    def on_data(self, data):
            if not flag.isSet():
                try:
                    tweet = json.loads(data)
                    # coordinates: [longitude, latitude]
                    document = {'text': tweet['text'],
                                'author': tweet['user']['screen_name'],
                                'keyword': getKey(tweet['text'], keySet),
                                'timestamp': parser.parse(tweet['created_at']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                'coordinates': tweet['place']['bounding_box']['coordinates'][0][0] } #  if 'place' in tweet and tweet['place'] else list(random.sample(positions, 1)[::-1])
                    socketio.emit('tweet', document, namespace='/streaming')
                    es.index(index='twittmap', doc_type='tweet', body=document)
                    print document['coordinates']
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

uploader = Thread(target=tweetStreaming)
uploader.setDaemon(True)

@application.route('/')
def start():
    return render_template('index.html')

@application.route('/query')
def query():
    key = request.args['keyword']
    lat = float(request.args['lat'] or 0)
    lng = float(request.args['lng'] or 0)
    if not key and not lat:
        print 'init'
        return json.dumps(tweets_init())
    if not key:
        print 'geo'
        return json.dumps(tweets_geo([lng, lat]))
    if not lat:
        print 'key'
        return json.dumps(tweets_key(key))
    print 'all'
    return json.dumps(tweets_all(key, [lng, lat]))

@socketio.on('connect', namespace='/streaming')
def connect():
    global flag
    flag = Event()
    global uploader
    if not uploader.isAlive():
        uploader.start()

@socketio.on('disconnect', namespace='/streaming')
def disconnect():
    #Set the internal flag to true. 
    #All threads waiting for it to become true are awakened.
    #Threads that call wait() once the flag is true will not block at all.
    flag.set() 
    print ("Socket Disconnected!")

if __name__ == '__main__':
    socketio.run(application, debug=True)
