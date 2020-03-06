import time
import sys
from json import loads
from os import path
from pyspark.storagelevel import StorageLevel
# from pyspark.sql.functions import
from pyspark.sql.types import *
from pyspark.sql.functions import *



if __name__ == "__main__":
    sys.path.append(path.dirname(__file__))

    from settings import default
    from configs.spark_config import socketDF as df
    from configs.spark_config import spark
    from jobs.transformation_job import TweetClass as tc

    
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

    # Returns True for DataFrames that have streaming sources
    print("("*40+str(df.columns)+")"*40)
    func = tc().process_tweets

    # tweet_dict = {"user": self.tweet.get('user', {})['name'],
    #               "location": self.tweet.get('user', {})['location'],
    #               "text": self.tweet.get("text")}
    def get_name(value):
        return loads(value).get('user', {})['name']

    def get_text(value):
        return loads(value).get('text', "--NA--")

    def get_location(value):
        return loads(value).get('user', {}).get('location', '--NA--')

    name = udf(get_name)
    location = udf(get_location)
    text = udf(get_text)
    # transform = df.writeStream.foreach(func)
    new = df.select("value")\
        .withColumn("name", name(df["value"]))\
        .withColumn("text", text(df["value"]))\
        .withColumn("location", location(df["value"]))

    print(new.columns)
    query = new \
        .writeStream \
        .outputMode('update') \
        .format('console') \
        .start()
    # transform = df.writeStream\
    #     .foreach(func)\
    #     .start()
    # print("&"*40+str(df.columns)+"*"*40)
    # df.createOrReplaceTempView("tweets")
    # query = spark.sql("select * from tweets")
    # for i in query .show():
    #     print(i)
    #
    #

    # You must start the Spark StreamingContext, and await process termination…
    query.awaitTermination()
    # transform.awaitTermination()

    # table = df.writeStream.queryName("tweets").start()
    # df.createOrReplaceTempView("tweets").start()
    # df2 = spark.sql("SELECT * from tweets")
    # for i in df2.collect():
    #     print(i, dir(i))

    # dtypes_map = {<class 'str'>: 'ShortType', <class 'NoneType'>: 'NullType', <class 'bool'>: 'BooleanType', <class 'datetime.date'>: 'DateType', <class 'datetime.datetime'>: 'TimestampType', <class 'float'>: 'FloatType', <class 'int'>: 'LongType', <class 'list'>: 'ArrayType', <class 'dict'>: 'MapType'}

    # table = df.write.saveAsTable("tweets").start()
    # table = df.select("value")
    # table = df.groupBy(
    #             df.value,
    #             window(current_timestamp(), "10 seconds")
    #         ).count()

    # table = df.writeStream \
    #             .format("parquet").outputMode("append") \
    #             .option("compression", "snappy") \
    #             .option("path",
    #                     "/home/qburst/Dev/open_source/QBContrib/PySparkCLI/s2/parquet/") \
    #             .option("checkpointLocation",
    #                     "/home/qburst/Dev/open_source/QBContrib/PySparkCLI/s2/checkpoint/") \
    #             .start()

    # table.cache()
    # table.createOrReplaceTempView("tweets")

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
