from pyspark.conf import SparkConf
from pyspark.context import SparkContext

## Supporting Functions


## Core Spark Application Functionality

def main(sc):
    print("Hello World!")

if __name__ == "__main__":
    # Configure Spark Application
    conf = SparkConf().setAppName("hello-world")
    conf = conf.setMaster("local[*]")
    conf.set("spark.executor.cores", 2)
    sc = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)