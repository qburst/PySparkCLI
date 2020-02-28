import time
import sys
from json import loads
from os import path
from pyspark.storagelevel import StorageLevel
# from pyspark.sql.functions import
from pyspark.sql.types import (StructField, StringType, ArrayType,
                               IntegerType, StructType)



if __name__ == "__main__":
    sys.path.append(path.join(path.dirname(__file__), '..'))
    from configs.spark_config import socketDF as df
    from settings import default
    from configs.spark_config import spark
    from jobs.transformation_job import TweetClass as tc


    def process_tweets(res):
        # print(result)
        # print(type(result))
        # # return {}
        result = loads(res[0])
        dict = {"user": result.get('user', {})['name'],
                "location": result.get('user', {})['location'],
                "text": result.get("text")}
        print(dict)
        return dict
    # ssc = spark_config.ssc
    # lines = ssc.socketTextStream(default.DATA_SOURCE, default.DATA_SOURCE_PORT)

    # When your DStream in Spark receives data, it creates an RDD every batch interval.
    # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
    # so that we have only one resulting part-00000 file in the directory.
    # The method saveAsTextFiles() should really be re-named saveInDirectory(),
    # because that is the name of the directory in which the final part-00000 file is saved.
    # We use time.time() to make sure there is always a newly created directory, otherwise
    # it will throw an Exception.

    # if default.DEBUG:
    #     lines.persist(StorageLevel.MEMORY_AND_DISK)
    
    # array_schema = StructType([
    #     StructField('user', StringType(), nullable=True),
    #     StructField('location', StringType(), nullable=True),
    #     StructField('text', StringType(), nullable=True)
    # ])
    # udf_struct = udf(lambda z: func(z), array_schema)
    # d=lines.flatMap(udf_struct)
    # print(type(d))
    # data = lines.map(lambda x: loads(x)).map(lambda result: {"user": result.get('user', {}).get('name', '--NA--'), "location": result.get('user', {}).get('location', '--NA--'), "text": result.get("text", "--NA--")})

    # data.saveAsTextFiles("./tweets/%f" % time.time())

    # if default.DEBUG:
    #     data.pprint()

    func = tc().process_tweets
    print(df.isStreaming)    # Returns True for DataFrames that have streaming sources
    df.printSchema()
    data = df.writeStream.format('memory') \
        .foreach(func)\
        .start()

    data.explain()
        # [{"user": result.get('user', {}).get('name', '--NA--'),
        #               "location": result.get('user', {}).get('location',
        #                                                      '--NA--'),
        #               "text": result.get("text", "--NA--")} for result in loads(x[0])]})\
        # .start()

    # spark.sql("select * from tweets").show()  # interactively query in-memory table

    # data = df.writeStream\
    #     .outputMode("complete")\
    #     .format("json")\
    #     .queryName('twitter_feed')\
    #     .start()
    # for j in [loads(i) for i in df.collect()]:
    #     print(j)

    # sqm = spark.streams
    # for q in sqm.active:
    #     print(type(q))
        # print(q.value)
    # d_ = data.flatMap(lambda x: loads(x)).map(lambda result: {"user": result.get('user', {}).get('name', '--NA--'), "location": result.get('user', {}).get('location', '--NA--'), "text": result.get("text", "--NA--")})
    # print(dir(data))
    # df.describe().show()
    # df.select('value').show(5)

    # df.writeStream.outputMode("complete") \
    # .option("checkpointLocation", "./tweets") \
    # .format("memory") \
    # .start()
    # if df.isStreaming:
    #     print("Starting data stream")
        # data = df.rdd.map(lambda x: loads(x)).map(lambda result: {"user": result.get('user', {}).get('name', '--NA--'), "location": result.get('user', {}).get('location', '--NA--'), "text": result.get("text", "--NA--")})
        # data.pprint()
    # ssc.start()
    # ssc.awaitTermination()

    # You must start the Spark StreamingContext, and await process termination…
    data.awaitTermination()