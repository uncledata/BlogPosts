from pyspark.sql.types import *
import time
from pyspark.sql import SparkSession
import json
import argparse


class MongoRunner:

    SCHEMA = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("job", StringType(), True),
            StructField("company", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("date_created", StringType(), True),
        ]
    )

    def __init__(self, partitioner, config_name, config_settings):
        self.partitioner = partitioner
        self.config_name = config_name
        self.config_settings = config_settings
        if partitioner == "MongoSinglePartitioner":
            self.spark = (
                SparkSession.builder.appName("Spark Benchmarking")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.driver.maxResultSize", "2g")
                .config(
                    "spark.jars.packages",
                    "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1",
                )
                .config(
                    "spark.mongodb.input.uri",
                    "mongodb://localhost:27017/mock_db.mock_collection",
                )
                .config("spark.mongodb.input.partitioner", partitioner)
                .getOrCreate()
            )
        else:
            self.spark = (
                SparkSession.builder.appName("Spark Benchmarking")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.driver.maxResultSize", "2g")
                .config(
                    "spark.jars.packages",
                    "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1",
                )
                .config(
                    "spark.mongodb.input.uri",
                    "mongodb://localhost:27017/mock_db.mock_collection",
                )
                .config("spark.mongodb.input.partitioner", partitioner)
                .config(config_name, config_settings)
                .getOrCreate()
            )

    def read_spark(self) -> None:
        start = time.time()
        v = (
            self.spark.read.schema(self.SCHEMA)
            .format("com.mongodb.spark.sql.DefaultSource")
            .load()
        )
        v.write.mode("overwrite").parquet("data/")
        with open("results.txt", "a") as f:
            vals = f"""{self.partitioner}, {self.config_name}, {self.config_settings}, {time.time() - start} \n"""
            f.write(vals)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--PARTITIONER", help="start time (ms)")
    parser.add_argument("--CONFIG_NAME", help="stop time (ms)")
    parser.add_argument("--CONFIG_VALUE", help="script name")
    args = parser.parse_args()
    MongoRunner(args.PARTITIONER, args.CONFIG_NAME, args.CONFIG_VALUE).read_spark()
