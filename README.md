# PySpark CLI

This will implement a PySpark Project boiler plate code based on user input.

Structuring of PySpark Application
==================================

We need following segments:
# Data Source:
    Where we would perform transform operations
    eg: text file, s3 file, hdfs path, shared folder etc
# Spark Configuration:
    Setting Up the configuration for our PySpark App

    eg:
        conf = SparkConf().setAppName("PythonWordCount").setMaster(local[*])
        sc = SparkContext(conf=conf)
# Caching or persisting data
    With paralllelize we can have the data cached in every worker node
# Implement Core logic like tranformations and collecting final data
    Here we will do transformation on the source data using map, flatMap etc

Add method and variable documentations
