from pyspark.sql.types import *
import time
from pyspark.sql import SparkSession
import json
import pyspark.sql.functions as f
import argparse


class SparkReader:
    def __init__(self, file_path: str, maxPartitionBytes: int, openCostInBytes: int):
        self.file_path = file_path
        self.maxPartitionBytes = maxPartitionBytes
        self.openCostInBytes = openCostInBytes
        self.spark = (
            SparkSession.builder.appName("Spark Benchmarking")
            .master("local[*]")
            .config("spark.driver.memory", "8g")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.sql.files.openCostInBytes", self.openCostInBytes)
            .config("spark.sql.files.maxPartitionBytes", self.maxPartitionBytes)
            .getOrCreate()
        )

    def read_head(self) -> None:
        spark_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .load(self.file_path + "*.csv")
        )
        new_df = spark_df.groupby("key").agg(f.mean(f.col("value")))
        print(new_df.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--FILE_PATH")
    parser.add_argument("--MAXPARTITIONBYTES")
    parser.add_argument("--OPENCOSTINBYTES")
    args = parser.parse_args()

    SparkReader(
        args.FILE_PATH, args.MAXPARTITIONBYTES, args.OPENCOSTINBYTES
    ).read_head()
