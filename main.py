import os
import time
from pyspark.sql import SparkSession


def load_input_data():
    global df
    if os.path.exists("out.parquet"):
        df = spark.read.load("out.parquet")
        print(f"Loaded {df.count()} records from file out.parquet")
    else:
        df = spark.read.text("out.txt")
        print(f"Loaded {df.count()} records from file out.txt")
        df.write.save("out.parquet", format="parquet")
        print(f"Saved dataframe to out.parquet")


def find_entry():
    return df.filter(df.value.contains(id)).count() > 0


if __name__ == "__main__":
    start_time = time.time()
    spark = SparkSession.builder.appName("S3Find").getOrCreate()

    load_input_data()
    print("Loading input data finished after %s seconds ---" % (time.time() - start_time))
    id = "14796571_20230428"
    found = find_entry()
    print(f"Found entry {id}" if found else f"Entry {id} not found")
    spark.stop()
    print("--- %s seconds ---" % (time.time() - start_time))
