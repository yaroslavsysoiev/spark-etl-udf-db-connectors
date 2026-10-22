from pyspark.sql import SparkSession, DataFrame

from spark_pipelines.config import (
    CSV_PATH,
    BRONZE_PATH,
    TABLE_NAME,
    jdbc_url,
    connection_properties
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
        url=jdbc_url,
        table=TABLE_NAME,
        properties=connection_properties
    )