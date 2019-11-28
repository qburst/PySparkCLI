# PySpark CLI

This will implement a PySpark Project boiler plate code based on user input.

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming.

PySpark is the Python API for Spark.

## Installation Steps:
    
    git clone https://github.com/qburst/PySparkCLI.git

    cd PySparkCLI

    pip install -e .
    
## Create a PySpark Project
    
    pysparkcli create [PROJECT_NAME] --master [MASTER_URL] --cores [NUMBER]

    master - The URL of the cluster it connects to. You can also use -m instead of --master.
    cores - You can also use -c instead of --cores.
            
## Run a PySpark Project
    
    pysparkcli run [PROJECT_NAME]

## Project Structure

The basic project structure is as follows:

```bash
sample
├── __init__.py
├── src
│   ├── app.py
│   ├── configs
│   │   ├── etl_config.json
│   │   └── __init__.py
│   ├── __init__.py
│   ├── jobs
│   │   ├── etl_job.py
│   │   └── __init__.py
│   └── settings
│       ├── default.py
│       ├── __init__.py
│       ├── local.py
│       └── production.py
└── tests
    ├── __init__.py
    ├── test_data
    │   ├── employees
    │   │   └── part-00000-9abf32a3-db43-42e1-9639-363ef11c0d1c-c000.snappy.parquet
    │   └── employees_report
    │       └── part-00000-4a609ba3-0404-48bb-bb22-2fec3e2f1e68-c000.snappy.parquet
    └── test_etl_job.py

8 directories, 15 files
```

## Contribution Guidelines

Check out [here](https://github.com/qburst/PySparkCLI/blob/master/CONTRIBUTING.md) for our contribution guidelines.

## Sponsors

[![QBurst](https://www.qburst.com/images/responsive/QBlogo.svg)](https://www.qburst.com)




