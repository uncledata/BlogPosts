from pyspark.sql.types import *
import time
from pyspark.sql import SparkSession
import json
import pyspark.sql.functions as f

spark = (
    SparkSession.builder.appName("Spark Benchmarking")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.driver.maxResultSize", "4g")
    .getOrCreate()
)

big_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("./data/KB/*.csv")
)
small_df = big_df.groupby("key").agg(f.mean(f.col("value")))
small_df.write()


joined = big_df.join(small_df, small_df.key == big_df.key, how="left")
