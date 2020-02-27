import json
from pyspark.sql import SparkSession


# Our transformation function:
def process_tweets(tweet):
    json_tweet = json.loads(tweet)
    print(json_tweet)
    spark = SparkSession.builder.getOrCreate()
    data_rdd = spark.read.json(json_tweet).rdd
    transformed_data = transformfunc(data_rdd)
    return transformed_data

def transformfunc(result):
    favCount = 0
    user = None
    if result["user"]["followers_count"]:
        if result["user"]['followers_count'] > favCount:
            favCount = result["user"]['followers_count']
            print(favCount)
            user = result["user"]["name"]
    return {"user": result['user']['name'], "location": result['user']['location'], "text": result["text"]}

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']