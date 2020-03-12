import os
import json


from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from pysparkcli.core.admin import SparkBuilder
from database.nosql_db import *
from database.sql_db import getSession, EtlData as SQLEtlData
from models.document import EtlData as MongoEtlData

BASE = os.path.dirname(os.path.realpath(__file__))

def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark = SparkBuilder("test").build_sc()
    config = {}
    with open(os.path.join(BASE, "configs/etl_config.json"), "r") as f:
        config = json.loads(f.read())

    # log that main ETL job is starting
    print('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config.get('steps_per_floor', 12))
    data = data_transformed.collect()
    load_data_to_mongo(data)
    load_data_to_postgres_db(data)
    load_data_as_parquet(data_transformed)

    # log the success and terminate Spark application
    print('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .parquet(os.path.join(BASE, '../tests/test_data/employees')))

    return df


def transform_data(df, steps_per_floor_):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
               (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

    return df_transformed


def load_data_as_parquet(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv(os.path.join(BASE,'../loaded_data'), mode='overwrite', header=True))
    return None


def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet(os.path.join(BASE, '../tests/test_data/employees'), mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet(os.path.join(BASE, '../tests/test_data/employees_report'), mode='overwrite'))

    return None


def load_data_to_mongo(data):
    for row in data:
        etl = MongoEtlData(name=row["name"], steps_to_desk=row["steps_to_desk"])
        etl.save()


def load_data_to_postgres_db(data):
    session = getSession()
    for row in data:
        etl = SQLEtlData(name=row["name"], steps_to_desk=row["steps_to_desk"])
        session.add(etl)
    session.commit()


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()