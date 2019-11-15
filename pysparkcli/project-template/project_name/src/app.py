from pyspark.conf import SparkConf
from pyspark.context import SparkContext

## Closure Functions


## Main functionality

def main(sc):
    tf = sc.textFile("pysparkcli/project-template/project_name/src/app.py")
    for i,j in enumerate(tf.collect()):
        print(i,j)
        if i==20:
            break
if __name__ == "__main__":
    # Configure Spark Application
    conf = SparkConf().setAppName("APP_NAME")
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
