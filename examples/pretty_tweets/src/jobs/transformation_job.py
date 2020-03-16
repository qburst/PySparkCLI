from json import loads
from pyspark.sql import SparkSession


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
            print(tweet_dict)
            # if default.DEBUG:
            #     pprint(tweet_dict)
            return tweet_dict
        except Exception as e:
            print("Error:"+e)
            return {}

def transformfunc(res):

    return [{"user": result.get('user', {})['name'], "location": result.get('user', {})['location'], "text": result.get("text")} for result in [row.asDict() for row in res.collect()] if result.get('user')]

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']