import os
import psycopg2
from psycopg2.extras import execute_batch
from pgvector.psycopg2 import register_vector

from pyspark.sql import DataFrame
from pyspark.sql.readwriter import DataFrameWriter

from typing import Iterator, Any

from spark_pipelines.movies_pipeline.config import (
    BRONZE_DELTA_PATH,
    SILVER_TABLE_NAME,
    JDBC_URL,
    CONNECTION_PROPERTIES
)


def write_to_delta(df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_DELTA_PATH)
    )


def jdbc_writer(
    df: DataFrame,
    table_name: str
) -> DataFrameWriter:
    return (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .options(**CONNECTION_PROPERTIES)
    )


def _insert_embeddings_partition(rows: Iterator[Any]) -> None:
    rows = list(rows)

    if not rows:
        return

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    register_vector(conn)

    try:
        with conn:
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    f"""
                    INSERT INTO {SILVER_TABLE_NAME}
                    (
                         show_id,
                         title,
                         country,
                         country_code,
                         description,
                         embedding,
                         ingestion_timestamp,
                         processed_timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (show_id)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        country = EXCLUDED.country,
                        country_code = EXCLUDED.country_code,
                        description = EXCLUDED.description,
                        embedding = EXCLUDED.embedding,
                        ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                        processed_timestamp = EXCLUDED.processed_timestamp
                    """,
                    [
                        (
                            row.show_id,
                            row.title,
                            row.country,
                            row.country_code,
                            row.description,
                            row.embedding,
                            row.ingestion_timestamp,
                            row.processed_timestamp
                        )
                        for row in rows
                    ],
                    page_size=1000,
                )
    finally:
        conn.close()


def write_embeddings_to_postgres(df: DataFrame) -> None:
    """
    Write embedding vectors directly to PostgreSQL using pgvector adapters.
    """

    df.foreachPartition(
        _insert_embeddings_partition
    )
