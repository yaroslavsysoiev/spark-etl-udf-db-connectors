import os

from pyspark.sql import SparkSession, DataFrame
from UDF import country_to_iso

# import pandas as pd
# import pycountry
#
# from pyspark.sql.functions import pandas_udf


spark = (
    SparkSession.builder
    .appName("NetflixETL")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/app/netflix_titles.csv")
)

print("======================================== ORIGINAL DATA ========================================")
df.show(5, truncate=True)
df.printSchema()

# @pandas_udf("string")
# def country_to_iso(countries: pd.Series) -> pd.Series:
#     def convert(value):
#         if pd.isna(value):
#             return None
#
#         result = []
#
#         for country_name in value.split(","):
#             country_name = country_name.strip()
#
#             try:
#                 country = pycountry.countries.lookup(country_name)
#                 result.append(country.alpha_2)
#             except LookupError:
#                 result.append("UNKNOWN")
#
#         return ",".join(result)
#
#     return countries.apply(convert)


df = df.withColumn("country_code", country_to_iso("country"))

jdbc_url = (
    f"jdbc:postgresql://"
    f"{os.environ['POSTGRES_HOST']}:5432/"
    f"{os.environ['POSTGRES_DB']}"
)

connection_properties = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

df.write.jdbc(
    url=jdbc_url,
    table="public.netflix_titles",
    mode="overwrite",
    properties=connection_properties,
)

print("Saved to PostgreSQL")

# Verify by reading it back
db_df = spark.read.jdbc(
    url=jdbc_url,
    table="public.netflix_titles",
    properties=connection_properties,
)

df.select("title", "country", "listed_in", "country_code").show()

spark.stop()
