import config
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import json
import logging

def authenticate():
    """Function for handling Twitter Authentication"""
    auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth

class TwitterListener(StreamListener):

    def on_data(self, data):

        """Whatever we put in this method defines what is done with
        every single tweet as it is intercepted in real-time"""

        t = json.loads(data) #t is just a regular python dictionary.

        tweet = {
        'text': t['text'],
        'username': t['user']['screen_name'],
        'followers_count': t['user']['followers_count']
        }

        logging.critical(f'\n\n\nTWEET INCOMING: {tweet["text"]}\n\n\n')

        client = MongoClient('mongodb')
        db = client.mongodb
        db.kungflu.insert(t)

    def on_error(self, status):

        if status == 420:
            print(status)
            return False

if __name__ == '__main__':

    auth = authenticate()
    listener = TwitterListener()
    stream = Stream(auth, listener)
    stream.filter(track=['kung flu', 'kungflu'], languages=['en'])
