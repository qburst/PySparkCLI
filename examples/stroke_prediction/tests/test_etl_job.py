"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
import json
import sys
import os


# Add to system path for pysparkcli modules
SRC_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../src/')
sys.path.append(os.path.join(SRC_PATH))

from pyspark.sql.functions import mean

from pysparkcli.core.admin import SparkBuilder
from jobs.etl_job import transform_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        with open(SRC_PATH + "configs/etl_config.json", "r") as f:
            self.config = json.loads(f.read())
        self.spark = SparkBuilder("test").build_sc()
        self.test_data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../tests/test_data/')

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees'))

        expected_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees_report'))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()
        expected_avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # act
        data_transformed = transform_data(input_data, 21)

        cols = len(expected_data.columns)
        rows = expected_data.count()
        avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()
