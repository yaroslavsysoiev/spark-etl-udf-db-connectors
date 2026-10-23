import os
import psycopg2
from psycopg2.extras import execute_batch
from pgvector.psycopg2 import register_vector

from pyspark.sql import DataFrame

import logging
from typing import Iterator

from spark_pipelines.movies_pipeline.config import (
    TABLE_NAME,
    jdbc_url,
    connection_properties
)

logger = logging.getLogger(__name__)

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


def _insert_embeddings_partition(rows: Iterator) -> None:
    rows = list(rows)

    if len(rows) == 0:
        return None

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
                    """
                    INSERT INTO public.netflix_titles_silver
                    (show_id,
                     title,
                     country,
                     country_code,
                     description,
                     embedding)
                    VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (show_id)
                                DO
                    UPDATE SET
                        title = EXCLUDED.title,
                        country = EXCLUDED.country,
                        country_code = EXCLUDED.country_code,
                        description = EXCLUDED.description,
                        embedding = EXCLUDED.embedding
                    """,
                    [
                        (
                            row.show_id,
                            row.title,
                            row.country,
                            row.country_code,
                            row.description,
                            row.embedding,
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
