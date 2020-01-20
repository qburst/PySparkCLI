
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# SparkContext(“local[1]”) would not work with Streaming, 2 threads are required
sc = SparkContext("local[2]", "Twitter Demo")
ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds
IP = "localhost"
Port = 5555
lines = ssc.socketTextStream(IP, Port)
