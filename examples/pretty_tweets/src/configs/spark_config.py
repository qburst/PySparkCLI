
from pyspark.sql import SparkSession

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