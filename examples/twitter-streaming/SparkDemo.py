# SparkDemo.py
# ﻿This code is copyright (c) 2017 by Laurent Weichberger.
# Authors: Laurent Weichberger, from Hortonworks and,
# from RAND Corp: James Liu, Russell Hanson, Scot Hickey,
# Angel Martinez, Asa Wilks, & Sascha Ishikawa
# This script does use Apache Spark. Enjoy...
# This code was designed to be run as: spark-submit SparkDemo.py

import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import database
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter


# Our filter function:
def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    # print("%"*40)
    # print(json_tweet.get('lang'))
    # print("&"*40)

    print("tweet")
    if json_tweet.get('lang'):  # When the lang key was not present it caused issues
        if json_tweet['lang'] == 'en':
            print("english tweet")
            print(json_tweet)
            print("end tweet")
            spark = SparkSession \
                .builder \
                .appName("Python Spark SQL data source example") \
                .getOrCreate()
            df = pd.DataFrame(json_tweet)
            ddf = spark.createDataFrame(df)
            spark.write.jdbc("jdbc:postgresql:"+db.url+"/"+db.database, "schema.twitterdata",
                          properties={"user": db.user, "password": db.password})

            return True  # filter() requires a Boolean value
    return False


# SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
sc = SparkContext("local[2]", "Twitter Demo")
ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds
IP = "localhost"
Port = 5555
lines = ssc.socketTextStream(IP, Port)
db = database.config()


# When your DStream in Spark receives data, it creates an RDD every batch interval.
# We use coalesce(1) to be sure that the final filtered RDD has only one partition,
# so that we have only one resulting part-00000 file in the directory.
# The method saveAsTextFile() should really be re-named saveInDirectory(),
# because that is the name of the directory in which the final part-00000 file is saved.
# We use time.time() to make sure there is always a newly created directory, otherwise
# it will throw an Exception.

lines.foreachRDD(lambda rdd: rdd.filter(filter_tweets).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))

ssc.start()
ssc.awaitTermination()
