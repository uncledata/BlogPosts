from pyspark.sql import SparkSession
import itertools
from pyspark.sql import DataFrame
import timeit
import os


def get_spark_session() -> SparkSession:
    return SparkSession.builder.master("local[8]").appName("convertor").config(
                    "spark.jars.packages",
                    "org.apache.spark:spark-avro_2.12:3.1.1",
                ).config("spark.driver.memory", "8g").getOrCreate()


def write_data(df: DataFrame,
               compression: str,
               file_format: str,
               prefix: str = "",
               read_comp: str = None,
               read_format: str = None) -> None:
    start = timeit.default_timer()
    if compression == "none" and file_format == "avro":
        (df
         .write
         .format(file_format)
         .option("header", "true")
         .mode("overwrite")
         .save(f"./data/{prefix}used_cars_data_none.{file_format}")
         )
    else:
        (df
         .write
         .format(file_format)
         .option("compression", compression)
         .option("header", "true")
         .mode("overwrite")
         .save(f"./data/{prefix}used_cars_data_{compression}.{file_format}")
         )
    stop = timeit.default_timer()
    if read_comp and read_format:
        print(f'{read_comp} and {read_format} time: ', stop - start)
    else:
        print(f'{compression} and {file_format} time: ', stop - start)


def read_csv_write_data(spark: SparkSession) -> None:
    df = (spark
          .read
          .format("csv")
          .load("./data/used_cars_data.csv", header="true")
          )

    compressions = ["none", "snappy", "gzip", "bzip2", "deflate"]
    formats = ["csv", "parquet", "avro", "orc"]

    for compression, file_format in list(itertools.product(compressions, formats)):
        try:
            write_data(df=df, compression=compression, file_format=file_format)
        except Exception as e:
            print(file_format, compression, e)
            continue


def read_format_write_csv(spark: SparkSession) -> None:
    subfolders = [fl.path for fl in os.scandir("./data/") if fl.is_dir() and "writing_" not in fl.path ]
    for folder in subfolders:
        extension = folder.split(".")[-1]
        compression = folder.split(".")[1].split("_")[-1]
        df = spark.read.format(extension).load(folder)
        write_data(df=df,
                   compression="none",
                   file_format="csv",
                   read_comp=compression,
                   read_format=extension,
                   prefix="writing_")


def register_as_hive_tables(spark: SparkSession) -> None:
    pass


def main():
    spark = get_spark_session()
    #read_csv_write_data(spark=spark)
    print("/********* READ FORMAT, WRITE CSV **************/")
    read_format_write_csv(spark=spark)
    #register_as_hive_tables()


if __name__ == '__main__':
    main()

"""
none and csv time:  54.422659546
none and parquet time:  114.932614868
none and avro time:  73.959692198
none and orc time:  89.73516376099997
snappy and parquet time:  82.68221760399996
snappy and avro time:  85.56519636800004
snappy and orc time:  90.48209155999996
gzip and csv time:  112.278987325
gzip and parquet time:  111.24731192699994
bzip2 and csv time:  397.32836831399993
bzip2 and avro time:  343.58149617000004
deflate and csv time:  102.1259878410001
deflate and avro time:  101.84209859899988


/********* READ FORMAT, WRITE CSV **************/
bzip2 and csv time:  207.511667168
gzip and parquet time:  38.36470992600002
snappy and orc time:  34.38275365000004
snappy and avro time:  38.08637959299995
deflate and csv time:  61.69582790300001
deflate and avro time:  43.53127027900001
none and csv time:  51.420189206999964
none and parquet time:  39.652601136000044
none and avro time:  36.95435313799999
snappy and parquet time:  40.505412232000026
gzip and csv time:  57.52004755200005
bzip2 and avro time:  109.1426772929999
none and orc time:  39.478590129000054
"""