
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from settings import default

sc = SparkContext(default.MASTER_URL, default.APP_NAME)
ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds