
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext

from settings import default

spark = SparkSession.builder \
    .appName(default.APP_NAME) \
    .master(default.MASTER_URL) \
    .getOrCreate()

jsonSchema = StructType([StructField("created_at", StringType(), True),
                         StructField("id", LongType(), True),
                         StructField("id_str", StringType(), True),
                         StructField("text", StringType(), True),
                         StructField("display_text_range", ArrayType(IntegerType()), True),
                         StructField("source", StringType(), True),
                         StructField("truncated", BooleanType(), True),
                         StructField("in_reply_to_status_id", NullType(), True),
                         StructField("in_reply_to_status_id_str", NullType(), True),
                         StructField("in_reply_to_user_id", NullType(), True),
                         StructField("in_reply_to_user_id_str", NullType(), True),
                         StructField("in_reply_to_screen_name", NullType(), True),
                         StructField("user", MapType(StringType(), StringType()), True),
                         StructField("geo", NullType(), True),
                         StructField("coordinates", NullType(), True),
                         StructField("place", NullType(), True),
                         StructField("contributors", NullType(), True),
                         StructField("is_quote_status", BooleanType(), True),
                         StructField("extended_tweet", MapType(StringType(), StringType()), True),
                         StructField("quote_count", LongType(), True),
                         StructField("reply_count", LongType(), True),
                         StructField("retweet_count", LongType(), True),
                         StructField("favorite_count", LongType(), True),
                         StructField("entities", MapType(StringType(), StringType()), True),
                         StructField("favorited", BooleanType(), True),
                         StructField("retweeted", BooleanType(), True),
                         StructField("possibly_sensitive", BooleanType(), True),
                         StructField("filter_level", StringType(), True),
                         StructField("lang", StringType(), True),
                         StructField("timestamp_ms", StringType(), True)])

# Read text from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", default.DATA_SOURCE) \
    .option("port", default.DATA_SOURCE_PORT) \
    .load()
    # .option("maxFilesPerTrigger", 1)\
# sc = SparkContext(default.MASTER_URL, default.APP_NAME)
# ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds