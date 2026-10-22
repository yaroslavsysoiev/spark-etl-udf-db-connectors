import os

from pyspark.sql import SparkSession, DataFrame

from spark_pipelines.config import (
    CSV_PATH,
    BRONZE_PATH,
    TABLE_NAME,
    jdbc_url,
    connection_properties
)

# Readers

def read_csv(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(CSV_PATH)
    )


def read_parquet(spark: SparkSession) -> DataFrame:
    return (
        spark.read.parquet(
            BRONZE_PATH
        )
    )


def read_from_postgres(spark: SparkSession) -> DataFrame:
    return spark.read.jdbc(
        url=jdbc_url,
        table=TABLE_NAME,
        properties=connection_properties
    )

# Writers

def write_to_parquet(df: DataFrame) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(TABLE_NAME)
    )


def write_to_postgres(df: DataFrame) -> None:
    df.write.jdbc(
        url=jdbc_url,
        table=TABLE_NAME,
        mode="overwrite",
        properties=connection_properties
    )


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

def write_aggregations_to_table(df: DataFrame) -> None:
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
