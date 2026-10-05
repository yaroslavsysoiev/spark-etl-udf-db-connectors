import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option('header', 'true').csv(
r'C:\Users\YaroslavSysoiev\PycharmProjects\PythonProject\netflix_titles.csv'
)

df.show(2)
