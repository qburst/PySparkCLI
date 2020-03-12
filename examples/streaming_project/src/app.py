import time
import sys
from json import loads
from os import path
from pyspark.storagelevel import StorageLevel
from mongoengine import *
connect("mydb2", alias='default', host='localhost:27017')

class Tweet(Document):
    user = StringField(required=True, max_length=200)
    location = StringField(required=False, max_length=500)
    meta = {'allow_inheritance': True}

def saveToDB(data):
    data = loads(data)
    user = data.get('user', {}).get('name', '--NA--')
    location = data.get('user', {}).get('location', '--NA--')
    tweet = Tweet(user=user, location=location)
    tweet.save()

if __name__ == "__main__":
    sys.path.append(path.join(path.dirname(__file__), '..'))
    from configs import spark_config

    ssc = spark_config.ssc
    lines = ssc.socketTextStream(spark_config.IP, spark_config.Port)

    # When your DStream in Spark receives data, it creates an RDD every batch interval.
    # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
    # so that we have only one resulting part-00000 file in the directory.
    # The method saveAsTextFiles() should really be re-named saveInDirectory(),
    # because that is the name of the directory in which the final part-00000 file is saved.
    # We use time.time() to make sure there is always a newly created directory, otherwise
    # it will throw an Exception.

    # lines.persist(StorageLevel.MEMORY_AND_DISK)
    #
    # data = lines.map(lambda x: loads(x)).map(lambda result: {"user": result.get('user', {}).get('name', '--NA--'), "location": result.get('user', {}).get('location', '--NA--'), "text": result.get("text", "--NA--")})
    #
    # data.saveAsTextFiles("./tweets/%f" % time.time())
    # data.pprint()
    # data = lines.map(lambda x: loads(x)).map(lambda result: saveToDB(result))
    lines.foreachRDD(lambda rdd: rdd.filter(saveToDB).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))
    # You must start the Spark StreamingContext, and await process termination…
    ssc.start()
    ssc.awaitTermination()