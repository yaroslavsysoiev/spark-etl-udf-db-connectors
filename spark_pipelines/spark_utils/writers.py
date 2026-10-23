import os
import psycopg2

from pyspark.sql import DataFrame

from pgvector.psycopg2 import register_vector
from psycopg2.extras import execute_batch

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


def write_embeddings_to_postgres(df: DataFrame) -> None:
    df.foreachPartition(
        _insert_embeddings_partition
    )


def _insert_embeddings_partition(rows):
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
