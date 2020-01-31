import time
import json
import sys
from os import path
from pyspark.storagelevel import StorageLevel
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    print(json_tweet)
    return True

if __name__ == "__main__":
    sys.path.append(path.join(path.dirname(__file__), "."))
    from configs import spark_config
    sc = spark_config.sc
    ssc = spark_config.ssc
    IP = "localhost"
    Port = 5555
    lines = ssc.socketTextStream(IP, Port)

    # When your DStream in Spark receives data, it creates an RDD every batch interval.
    # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
    # so that we have only one resulting part-00000 file in the directory.
    # The method saveAsTextFile() should really be re-named saveInDirectory(),
    # because that is the name of the directory in which the final part-00000 file is saved.
    # We use time.time() to make sure there is always a newly created directory, otherwise
    # it will throw an Exception.

    # lines.persist(StorageLevel.MEMORY_AND_DISK)

    # counts = lines.filter(filter_tweets)
    # counts.pprint()  
    lines.foreachRDD(lambda rdd: rdd.filter(filter_tweets).coalesce(1).saveAsTextFile(
        "./tweets/%f" % time.time()))

    # You must start the Spark StreamingContext, and await process termination…
    ssc.start()
    ssc.awaitTermination()