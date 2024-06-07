import os
import subprocess
import sys
import time
from pyspark.sql import SparkSession


def find_entry(id, df):
    return df.filter(df.value == id).count() > 0


def spark_find(test_file):
    start_time = time.time()
    spark = SparkSession.builder.appName("S3Find").getOrCreate()
    df = spark.read.text("test_output")
    with open(test_file, "r") as f:
        for line in f:
            id_to_find = line.rstrip()
            find_entry(id_to_find, df)
    spark.stop()
    print("SPARK: --- %s seconds ---" % (time.time() - start_time))

def grep_find(test_file):
    start_time = time.time()
    with open(test_file, "r") as f:
        for line in f:
            id_to_find = line.rstrip()
            subprocess.call(['/usr/bin/grep', '-q', id_to_find, 'test_output'])
    print("GREP: --- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":

    numOfRecords = str(sys.argv[1])
    spark_find("test_outputtest"+numOfRecords)
    grep_find("test_outputtest"+numOfRecords)

