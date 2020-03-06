import time
import sys
from json import loads
from os import path
from pyspark.storagelevel import StorageLevel

from settings import default
from jobs.transformation_job import TweetClass as tc


if __name__ == "__main__":
    sys.path.append(path.join(path.dirname(__file__), '..'))
    from configs.spark_config import socketDF as df

    if df.isStreaming:
        func = tc().process_tweets
        transform = df.writeStream\
            .foreach(func)\
            .start()

        transform.awaitTermination()
