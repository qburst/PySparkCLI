from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
## Supporting Functions


## Core Spark Application Functionality

def main(sc):
    print("Hello!!!!!")

if __name__ == "__main__":
    # Configure Spark Application
    conf = SparkConf().setAppName("stroke_prediction")
    conf = conf.setMaster("local[*]")
    conf.set("spark.executor.cores", 2)
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.appName('strokeprediction').getOrCreate()

    # Execute Main functionality
    main(sc)