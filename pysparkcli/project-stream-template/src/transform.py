from pyspark.sql import SparkSession


def transform(dataRDD):
    lists = dataRDD.collect()
    return lists
