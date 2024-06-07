import os
import sys
import time
from pyspark.sql import SparkSession


def load_input_data(spark):
    if os.path.exists("out.parquet"):
        df = spark.read.load("out.parquet")
        print(f"Loaded {df.count()} records from file out.parquet")
    else:
        df = spark.read.text("out.txt")
        print(f"Loaded {df.count()} records from file out.txt")
        df.write.save("out.parquet", format="parquet")
        print(f"Saved dataframe to out.parquet")
    return df


def find_entry(id, df):
    return df.filter(df.value == id).count() > 0


def find_in_s3(id_to_find):
    spark = SparkSession.builder.appName("S3Find").getOrCreate()
    df = load_input_data(spark)
    print("Loading input data finished after %s seconds ---" % (time.time() - start_time))
    found = find_entry(id_to_find, df)
    print(f"Found entry {id_to_find}" if found else f"Entry {id_to_find} not found")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    id_to_find = sys.argv[1]
    start_time = time.time()
    find_in_s3(id_to_find)
    print("--- %s seconds ---" % (time.time() - start_time))
