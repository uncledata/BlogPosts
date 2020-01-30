from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from datetime import datetime
from pyspark.sql.types import *
import pandas as pd
import pyspark.sql.functions as f 
import json
import time 

def read_file_v1(spark: SparkSession) -> SparkDataFrame:

    schema = StructType([StructField('id', StringType(), True),
                         StructField('index', IntegerType(),True),
                         StructField('applications', 
                                     ArrayType(StructType([
                                         StructField("app_id", IntegerType(), False),
                                         StructField("app_creator_name", StringType(), False),
                                                                        ])
                                               ), True),])
    
    df = spark.read.option("multiLine", True).schema(schema).json('./data.json')
    return df

def read_file_v2(spark: SparkSession) -> SparkDataFrame:

    schema = StructType([StructField('id', StringType(), True),
                         StructField('index', IntegerType(),True),
                         StructField('applications', StringType(), True),])
    
    df = spark.read.option("multiLine", True).schema(schema).json('./data.json')
    return df

def exploding(spark: SparkSession) -> None:
    df = read_file_v1(spark)
    df = df.withColumn("application", f.explode(f.col("applications")))\
        .selectExpr("id", "application.app_id as app_id", "application.app_creator_name as app_creator_name")\
        .filter(f.col("app_creator_name")=="Dotson Harvey")\
        .select("id").distinct()
    df.write.mode("overwrite").parquet("./output/app_data.parquet")

def regexp(spark: SparkSession) -> None:
    df = read_file_v2(spark)
    df = df.withColumn("first_app_id", f.regexp_extract(f.col("applications"),
                       '(\{"app_id":)(\d{1})(,"app_creator_name":"Dotson Harvey"\})',
                       2
                       ))\
                           .filter(f.col("first_app_id")!='')\
                           .select("id")
    df.write.mode("overwrite").parquet("./output/app_data.parquet")
    

def main():
    spark = SparkSession.builder \
        .appName("Spark Benchmarking") \
        .master('local[*]') \
        .config('spark.driver.memory', '4g')\
        .config("spark.driver.maxResultSize", "2g")\
        .getOrCreate()
    runtimes = []

    for i in range(0, 10):
        start = time.time()
        exploding(spark)
        t = time.time() - start
        runtimes.append(t) 
    print("avg with explode: ",sum(runtimes)/len(runtimes))
    runtimes = []
    for i in range(0, 100):
        start = time.time()
        regexp(spark)
        t = time.time() - start
        runtimes.append(t) 
    print("avg with regexp: ",sum(runtimes)/len(runtimes))

if __name__ == '__main__':
    main()