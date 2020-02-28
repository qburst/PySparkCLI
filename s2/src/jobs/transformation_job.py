from json import loads
from pprint import pprint
from pyspark.sql import SparkSession

from settings import default


class TweetClass:

    def __init__(self):
        self.tweet = {}

# Our transformation function:
    def process_tweets(self, data):
        print(type(data), dir(data))
        # Retrieving dataframe Row object here with values as string
        self.tweet = loads(data[0])
        tweet_dict = {"user": self.tweet.get('user', {})['name'],
                      "location": self.tweet.get('user', {})['location'],
                      "text": self.tweet.get("text")}
        # if default.DEBUG:
        #     pprint(tweet_dict)
        return tweet_dict


def transform_func(res):

    return [{"user": result.get('user', {})['name'], "location": result.get('user', {})['location'], "text": result.get("text")} for result in [row.asDict() for row in res.collect()] if result.get('user')]

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']
