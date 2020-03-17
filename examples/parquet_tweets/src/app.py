import sys
from os import path


if __name__ == "__main__":
    CURR_DIR = path.dirname(__file__)
    sys.path.append(path.join(CURR_DIR, '..'))
    from configs.spark_config import socketDF as df

    table = df.writeStream \
                .format("parquet").outputMode("append") \
                .option("compression", "snappy") \
                .option("path",
                        path.join(CURR_DIR+"/../parquet/")) \
                .option("checkpointLocation",
                        path.join(CURR_DIR+"/../checkpoint/")) \
                .start()

    table.awaitTermination()
