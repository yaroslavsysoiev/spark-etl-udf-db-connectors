import os

from pyspark.sql import SparkSession, DataFrame

from spark.config import jdbc_url
from spark.config import (
    BRONZE_PATH
)


def read_parquet(spark: SparkSession) -> DataFrame:
    return (
        spark.read.parquet(
            BRONZE_PATH
        )
    )

print(f"jdbc_url={jdbc_url}")
print(f"POSTGRES_HOST={os.environ.get('POSTGRES_HOST')}")
print(f"POSTGRES_DB={os.environ.get('POSTGRES_DB')}")
print(f"POSTGRES_USER={os.environ.get('POSTGRES_USER')}")

def write_to_staging_table(df: DataFrame) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.netflix_titles_silver_staging")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def save_aggregations_to_table(df: DataFrame) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.netflix_titles_aggregated_table")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )