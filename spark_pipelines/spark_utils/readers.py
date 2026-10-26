from pyspark.sql import SparkSession, DataFrame

from spark_pipelines.movies_pipeline.config import (
    BRONZE_PATH ,
    JDBC_URL,
    CONNECTION_PROPERTIES
)


def read_csv(
        spark: SparkSession,
        path: str
) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def read_parquet(spark: SparkSession) -> DataFrame:
    return (
        spark.read.parquet(
            BRONZE_PATH
        )
    )


def read_from_postgres(spark: SparkSession) -> DataFrame:
    return spark.read.jdbc(
        url=JDBC_URL,
        table=table_name ,
        properties=CONNECTION_PROPERTIES
    )
