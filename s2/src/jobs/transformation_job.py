from json import loads
from pprint import pprint
from pyspark.sql import SparkSession

from settings import default


class TweetClass:

    def __init__(self):
        self.tweet = {}

    # Our transformation function:
    def process_tweets(self, data):
        # Retrieving dataframe Row object here with values as string
        try:
            self.tweet = loads(data.asDict()['value'])
            tweet_dict = {"user": self.tweet.get('user', {})['name'],
                          "location": self.tweet.get('user', {})['location'],
                          "text": self.tweet.get("text")}
            if default.DEBUG:
                pprint(tweet_dict)
            return tweet_dict
        except Exception as e:
            if default.DEBUG:
                print("Error:"+e)
            return {}


def transform_func(res):

    return [{"user": result.get('user', {})['name'], "location": result.get('user', {})['location'], "text": result.get("text")} for result in [row.asDict() for row in res.collect()] if result.get('user')]

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']
