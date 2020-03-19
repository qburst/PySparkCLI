import time
import sys
from datetime import datetime
from json import loads
from os import path
from mongoengine import DynamicDocument, StringField, connect, DateTimeField


class TwitterData(DynamicDocument):
    text = StringField(required=True, max_length=200)
    created_at = DateTimeField(default=datetime.utcnow())
    meta = {'allow_inheritance': True}

def saveMOngo(data):
    connect('streamdb')
    data = loads(data)
    text = data.get('text', '--NA--')

    if not text.startswith("RT @") and filterKeyword(text) and "retweeted_status" not in data.keys():
        hashtags = data.get('entities', {}).get('hashtags', [])
        if filterHash(hashtags):
            etl = TwitterData(text=text)
            etl.hashtags = hashtags
            etl.user = data.get('user')
            etl.save()


def filterHash(hashtags):
    filterTags = ['COVID19', 'coronavirus', 'Corona', 'CoronaVirusUpdate']
    for hashtag in hashtags:
        if hashtag.get("text") in filterTags:
            return True
    return False


def filterKeyword(text):
    textLower = text.lower()
    keywords = ['infected', 'recovered', 'infect', 'recover', 'death', 'died', 'cases', 'case', 'toll', 'cure', 'vaccine', 'travel history']
    for keyword in keywords:
        if keyword in textLower:
            return True
    return False

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
    lines.foreachRDD(lambda rdd: rdd.filter(saveMOngo).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))
    # You must start the Spark StreamingContext, and await process termination…
    ssc.start()
    ssc.awaitTermination()