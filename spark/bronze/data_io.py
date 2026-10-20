from pyspark.sql import SparkSession, DataFrame

from spark.bronze.config import (
    CSV_PATH,
    TABLE_NAME,
    jdbc_url,
    connection_properties
)


def read_csv(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(CSV_PATH)
    )


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


def read_from_postgres(spark: SparkSession) -> DataFrame:
    return spark.read.jdbc(
        url=jdbc_url,
        table=TABLE_NAME,
        properties=connection_properties
    )
