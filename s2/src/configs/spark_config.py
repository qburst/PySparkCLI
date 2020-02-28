
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

from settings import default

spark = SparkSession.builder \
    .appName(default.APP_NAME) \
    .master(default.MASTER_URL) \
    .getOrCreate()

# Read text from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", default.DATA_SOURCE) \
    .option("port", default.DATA_SOURCE_PORT) \
    .load()
# sc = SparkContext(default.MASTER_URL, default.APP_NAME)
# ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds