# PySpark CLI

This will implement a PySpark Project boiler plate code based on user input.

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming.

PySpark is the Python API for Spark.

## Installation Steps:
    
    git clone https://github.com/jino-qb/pysparkcli.git

    cd pysparkcli

    pip install .
    
## Create a PySpark Project
    
    pysparkcli create --master [MASTER_URL] --name [PROJECT_NAME] --cores [NUMBER]

    master - The URL of the cluster it connects to. You can also use -m instead of --master.
    name - The name of your PySpark project. You can also use -n instead of --name.
    cores - You can also use -c instead of --cores.
            
## Run a PySpark Project
    
    pysparkcli run -n [PROJECT_NAME]

    n - The name of your PySpark project.


## Project Structure

The basic project structure is as follows:

```bash
sample
├── __init__.py
├── src
│   ├── app.py
│   ├── configs
│   │   └── __init__.py
│   ├── __init__.py
│   └── settings
│       ├── default.py
│       ├── __init__.py
│       ├── local.py
│       └── production.py
└── tests
    └── __init__.py
```







