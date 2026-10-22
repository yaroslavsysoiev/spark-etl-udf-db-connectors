import os

from pyspark.sql import DataFrame

from spark_pipelines.movies_pipeline.config import (
    TABLE_NAME,
    jdbc_url,
    connection_properties
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

def jdbc_writer(df, table_name: str):
    return (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option(
            "user",
            os.environ["POSTGRES_USER"]
        )
        .option(
            "password",
            os.environ["POSTGRES_PASSWORD"]
        )
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .option(
            "dbtable",
            table_name
        )
    )