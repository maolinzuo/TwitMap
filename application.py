import json
from flask import Flask, jsonify, render_template, request
from tweetUtil import tweets_geo, tweets_key, tweets_all, tweetStreaming
from threading import Thread

application = Flask(__name__)

@application.route('/')
def start():
    return render_template('index.html')

@application.route('/query')
def query():
    key = request.args['keyword']
    lat = float(request.args['lat'] or 0)
    lng = float(request.args['lng'] or 0)
    if not key:
        return jsonify(tweets_geo([lng, lat]))
    if not lat:
        return json.dumps(tweets_key(key))
    return json.dumps(tweets_all(key, [lng, lat]))

if __name__ == '__main__':
    uploader = Thread(target=tweetStreaming)
    uploader.setDaemon(True)
    uploader.start()
    application.run()
