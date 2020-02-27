import time
import sys
from json import loads
from os import path
from pyspark.storagelevel import StorageLevel

from settings import default


if __name__ == "__main__":
    sys.path.append(path.join(path.dirname(__file__), '..'))
    from configs import spark_config

    ssc = spark_config.ssc
    lines = ssc.socketTextStream(default.DATA_SOURCE, default.DATA_SOURCE_PORT)

    # When your DStream in Spark receives data, it creates an RDD every batch interval.
    # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
    # so that we have only one resulting part-00000 file in the directory.
    # The method saveAsTextFiles() should really be re-named saveInDirectory(),
    # because that is the name of the directory in which the final part-00000 file is saved.
    # We use time.time() to make sure there is always a newly created directory, otherwise
    # it will throw an Exception.

    if default.DEBUG:
        lines.persist(StorageLevel.MEMORY_AND_DISK)

    data = lines.map(lambda x: loads(x)).map(lambda result: {"user": result.get('user', {}).get('name', '--NA--'), "location": result.get('user', {}).get('location', '--NA--'), "text": result.get("text", "--NA--")})

    data.saveAsTextFiles("./tweets/%f" % time.time())
    
    if default.DEBUG:
        data.pprint()

    # You must start the Spark StreamingContext, and await process termination…
    ssc.start()
    ssc.awaitTermination()