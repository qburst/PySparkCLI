# PySpark CLI

This will implement a PySpark Project boiler plate code based on user input.

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming.

PySpark is the Python API for Spark.

## Installation Steps:
    
    git clone https://github.com/qburst/PySparkCLI.git

    cd PySparkCLI

    pip3 install -e . --user
    
## Create a PySpark Project
    
    pysparkcli create [PROJECT_NAME] --master [MASTER_URL] --cores [NUMBER]

    master - The URL of the cluster it connects to. You can also use -m instead of --master.
    cores - You can also use -c instead of --cores.
            
## Run a PySpark Project
    
    pysparkcli run [PROJECT_NAME]

## Initiate Stream for Project

    pysparkcli stream [PROJECT_NAME] [STREAM_FILE_NAME]
    
## PySpark Project Test cases
    
   * Running by **Project name**
     
    pysparkcli test [PROJECT_NAME]
   * Running individual test case with filename: **test_etl_job.py**
   
    pysparkcli test [PROJECT_NAME] -t [etl_job]
    
## FAQ

Common issues while installing pysparkcli:

    * pysparkcli: command not found
        Make sure you add user’s local bin to PATH variable.
        Add the following code in .bashrc file

        # set PATH so it includes user's private bin if it exists
        if [ -d "$HOME/.local/bin" ] ; then
            PATH="$HOME/.local/bin:$PATH"
        fi


    * JAVA_HOME is not set
        Make sure JAVA_HOME is pointing to your JDK and PYSPARK_PYTHON variable is created.
        You can add them manually by in .bashrc file:
        
        Example:

            export PYSPARK_PYTHON=python3
            export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

        Save the file and run the following to update environment.

            source ~/.bashrc

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
## PySparkCLI Demo

[![PySparkCLI Demo](https://img.youtube.com/vi/wuoBKJYSfTE/0.jpg)](https://www.youtube.com/watch?v=wuoBKJYSfTE)

## Contribution Guidelines

Check out [here](https://github.com/qburst/PySparkCLI/blob/master/CONTRIBUTING.md) for our contribution guidelines.

## Sponsors

[![QBurst](https://www.qburst.com/images/responsive/QBlogo.svg)](https://www.qburst.com)
