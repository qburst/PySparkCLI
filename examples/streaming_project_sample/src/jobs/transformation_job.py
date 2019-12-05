import json
from pyspark.sql import SparkSession


# Our filter function:
def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    # print("%"*40)
    # print(json_tweet.get('lang'))
    # print("&"*40)
    # if json_tweet.get('lang'):  # When the lang key was not present it caused issues
    #     if json_tweet['lang'] == 'en':
    #         return True  # filter() requires a Boolean value
    # return False
    spark = SparkSession.builder.getOrCreate()
    data_rdd = spark.read.json(json_tweet).rdd
    transformed_data = transformfunc(data_rdd)
    if transformed_data['favcount'] > 20:
        return True
    return False

def transformfunc(dataRDD):
    results = dataRDD.collect()
    favCount = 0
    user = None
    for result in results:
        # print("main", result)
        if result["user"]["followers_count"]:
            if result["user"]['followers_count'] > favCount:
                favCount = result["user"]['followers_count']
                print(favCount)
                user = result["user"]["name"]
    return {"user": user, "favcount": favCount}



