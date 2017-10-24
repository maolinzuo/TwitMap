# TwittMap

COMS 6998 Homework1. 
Web application collects Twitts and does some processing and represents the Twitts on GoogleMaps.


## Features
1. Use Twitter Streaming API to fetch tweets from the twitter hose in real-time.
2. Use AWS Elastic Search to store the tweets on the back-end.
3. When opening the web, the web will display at most 500 markers in Google Map pulled from back-end. And user is able to choose a keyword from the dropdown and the corresponding tweets will present in the map.
4. User is able to search for tweets in a 500km-radius circle by clicking on the map(the click point will be the center).
5. While user is still connected to the server, if new tweet coming from Twitter Streaming API meets the requirements( keyword matching and posted in the selected area), new marker will show on the map.
6. User is able to view the text, username and timestamp of the tweets by clicking the marker.

## Getting Started

### Prerequisites

python2.7 virtualenv

### Installing

recreate environment

```
$virtualenv ~/.flaskhw01
$source ~/.flaskhw01/bin/activate
$pip install -r requirements.txt
```

run server
```
fill aws & es credentials in tweetUtil.py
$python application.py
```

## Deployment

cd to the top level directory
```
$eb init -p python2.7 <application name>
$eb create <environment name>
$eb open
```

## Project structure
### tweetUtil.py: contains helper functions of Elastic Search
### application.py: flask application entry point, contains view functions and streaming thread feature which utilized SocketIO
### static/bundle.js: a static js script generated by React webpack
### templates/index.html: jinjia2 template for main page
## Built With

* [Flask](http://flask.pocoo.org) - The web framework used
* [React](https://reactjs.org) - A JavaScript library for building user interfaces
* [Google Map](https://developers.google.com/maps/documentation/javascript/) - The map integrated into React
* [Webpack](https://webpack.github.io) - A module bundler



## Authors
Zhangyu Liu
Maolin Zuo


