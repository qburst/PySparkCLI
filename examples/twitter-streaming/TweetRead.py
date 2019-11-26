# TweetRead.py
# This first python script doesn’t use Spark at all:
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

# consumer_key = os.environ['TWITTER_CONSUMER_KEY']
# consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
# access_token = os.environ['TWITTER_ACCESS_TOKEN']
# access_secret = os.environ['TWITTER_ACCESS_SECRET']
consumer_key = 'DQKMfBZm1XqyarLVHjelFoxP1'
consumer_secret = '5G4PM3WcP0uMxu3pYEeNFiyfdF6lO051YBOqv5kseGMTNdhQ5k'
access_token = '1195273905688072192-djGQjpFRI9orIAMGPLmf4R1vj8O0TC'
access_secret = 'jfFtrE0ZcSUZKoOsvxWg8LYH1e2pOohiu8i0fFdjD45ha'

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            print(data.split('\n'))
            self.client_socket.send(bytes(data, "utf-8"))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            raise e
        # return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 5555  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.

    print("Received request from: " + str(addr))

    sendData(c)
