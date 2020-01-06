from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
import pandas as pd
import pyspark.sql.functions as f 

import time


import glob
import shutil


def function_to_apply_row(x, y):
    return x*y



def benchmark_read(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    for i in range(runs):
        start = time.time()
        df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
        print(df.count())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def benchmark_column_func_calc(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
    print(df.count())
    for i in range(runs):
        start = time.time()
        df = df.withColumn("cost",function_to_apply_row(f.col("Resource Quantity"),f.col("Resource Unit Price")))
        print(df.count())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))
    

def benchmark_column_col_calc(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
    print(df.count())
    for i in range(runs):
        start = time.time()
        df = df.withColumn("cost",f.col("Resource Quantity")*f.col("Resource Unit Price"))
        print(df.count())
        t = time.time() - start
        times.append(t) 
    del df
    return str(sum(times)/len(times))

def benchmark_write_pandas(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
    print(df.count())
    for i in range(runs):
        start = time.time()
        df.toPandas().to_csv("./output/output.csv")
        t = time.time() - start
        times.append(t) 
    return str(sum(times)/len(times))
    del df

def benchmark_single_file_rename(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
    print(df.count())
    for i in range(runs):
        start = time.time()
        df.coalesce(1).write.format('csv').mode("overwrite").save(f"./output/output_folder")
        for filename in glob.glob('./output/output_folder/*.csv'):
            shutil.move(filename, './output/output.csv')
        t = time.time() - start
        times.append(t) 
    return str(sum(times)/len(times))
    del df

def group_by(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
    print(df.count())
    for i in range(runs):
        start = time.time()
        new_df = df.groupby("Resource Vendor Name").agg({"Resource Quantity" : 'mean'})
        print(new_df.count())
        t = time.time() - start
        times.append(t) 
        del new_df
    return str(sum(times)/len(times))
    del df
    
def fill_na(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
    print(df.count())
    for i in range(runs):
        start = time.time()
        new_df = df.na.fill('0')
        print(new_df.count())
        t = time.time() - start
        times.append(t)
        del new_df 
    return str(sum(times)/len(times))
    del df
    
def col_calc_v1(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    for i in range(runs):
        start = time.time()
        df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
        df = df.withColumn("cost",f.col("Resource Quantity")*f.col("Resource Unit Price"))
        df.coalesce(1).write.format('csv').mode("overwrite").save(f"./output/output_folder")
        for filename in glob.glob('./output/output_folder/*.csv'):
            shutil.move(filename, './output/output.csv')
        t = time.time() - start
        times.append(t) 
    return str(sum(times)/len(times))
    del df
    
def col_calc_v2(spark:SparkSession, runs:int, file_name:str, schema):
    times = []
    for i in range(runs):
        start = time.time()
        df = spark.read.load(file_name, format="csv", counter='true', schema=schema)
        df = df.withColumn("cost",f.col("Resource Quantity")*f.col("Resource Unit Price"))
        df.coalesce(1).write.format('csv').mode("overwrite").save(f"./output/output_folder")
        for filename in glob.glob('./output/output_folder/*.csv'):
            shutil.move(filename, './output/output.csv')
        t = time.time() - start
        times.append(t) 
    return str(sum(times)/len(times))
    del df
    
def main():
    spark = SparkSession.builder \
        .appName("Spark Benchmarking") \
        .master('local[*]') \
        .config('spark.driver.memory', '16g')\
        .config("spark.driver.maxResultSize", "6g")\
        .getOrCreate()
    num = 10
    schema = StructType([StructField('Project ID', StringType(), True),
                         StructField('Resource Item Name', StringType(),True),
                         StructField('Resource Quantity', DecimalType(), True),
                         StructField('Resource Unit Price', DecimalType(), True),
                         StructField('Resource Vendor Name', StringType(), True),])
    file_name = ['./data/Resources.csv', './data/resources_v2.csv']
    
    t1 = []
    t2 = []
    t3 = []
    t4 = []
    t5 = []
    t6 = []

    for f in file_name:
        t1.append(col_calc_v1(spark, num, f, schema))
        t2.append(col_calc_v1(spark, num, f, schema))
    """
        t1.append(benchmark_read(spark, num, f, schema))
        t2.append(benchmark_column_func_calc(spark, num, f, schema))
        t3.append(benchmark_column_col_calc(spark, num, f, schema))
        t4.append(benchmark_single_file_rename(spark, num, f, schema))
        t5.append(group_by(spark, num, f, schema))
        t6.append(fill_na(spark, num, f, schema))
    """
    for i in range(len(t1)):
        print(f"""
              File_name: {file_name[i]}
              col * col: {t1[i]}
              func(col,col): {t2[i]}
              """)
       # print(f"""
        #    File_name: {file_name[i]}
         #   Read: {t1[i]} 
          #  Col with func: {t2[i]}
           # Col with calc: {t3[i]}
            #write single partition: {t4[i]}
            #Group by: {t5[i]}
            #fill na: {t6[i]}
        #""")


if __name__ == '__main__':
    main()