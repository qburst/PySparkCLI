import unittest
from pyspark.sql import SparkSession
from os import path
import json


class SparkStreamTest(unittest.TestCase):

    def setUp(self):
        spark = SparkSession.builder.getOrCreate()
        self.dataRDD = spark.read.json(path.join(path.dirname(path.realpath(__file__)), '../tests/test-data/inputs/data.json')).rdd

    def test_transform_data(self):
        result = transformation_job.transformfunc(self.dataRDD)
        print(result)


if __name__ == '__main__':
    import sys
    sys.path.append(path.join(path.dirname(__file__), '..'))
    from src.jobs import transformation_job
    unittest.main()

