import subprocess
from os import system
# -*- coding: utf-8 -*-
import sys
import os

P = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../src")
sys.path.append(P)
print(sys.path)
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from streaming import spark_stream


## Supporting Functions


## Core Spark Application Functionality

def main(sc):
    print("fxnkjasn")


if __name__ == "__main__":
    # Configure Spark Application
    conf = SparkConf().setAppName("sample")
    conf = conf.setMaster("local")
    conf.set("spark.executor.cores", 2)
    sc = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
