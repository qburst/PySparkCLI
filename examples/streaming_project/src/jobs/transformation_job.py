import json
from pyspark.sql import SparkSession


# Our transformation function:
def processTweets(tweet):
    json_tweet = json.loads(tweet)
    print(json_tweet)
    spark = SparkSession.builder.getOrCreate()
    data_rdd = spark.read.json(json_tweet).rdd
    transformed_data = transformfunc(data_rdd)
    return transformed_data

def transformFunc(result):
    return {"user": result.get('user', {})['name'], "location": result.get('user', {})['location'], "text": result["text"]}

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']