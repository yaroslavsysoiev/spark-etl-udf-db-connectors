import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, to_json, col, explode, split, trim
from pyspark.sql.types import ArrayType, FloatType

import pandas as pd
import psycopg2

from country_to_iso_udf import country_to_iso
from support_embedding_udf import embedding_udf
# the second pandas_udf -> to be moved to a different module -----------------------------------------------------------

EMBED_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


# @pandas_udf(ArrayType(FloatType()))
# def embedding_udf(texts: pd.Series) -> pd.Series:
#     if not hasattr(embedding_udf, "_model"):
#         from sentence_transformers import SentenceTransformer
#
#         embedding_udf._model = SentenceTransformer(
#             EMBED_MODEL
#         )
#
#     vectors = embedding_udf._model.encode(
#         texts.tolist(),
#         batch_size=32,
#     )
#
#     return pd.Series(
#         [vector.tolist() for vector in vectors]
#     )


# Finally writing the embedded description to a new table netflix_titles_silver ----------------------------------------
def load_embeddings_to_pgvector():
    """
    Load data from the staging table into the final Silver table.

    Assumptions:
    - pgvector extension already exists.
    - netflix_titles_silver table already exists.
    - HNSW index already exists.
    - All schema objects are managed by SQL migration scripts.
    """

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                            INSERT INTO public.netflix_titles_silver
                            (show_id,
                             title,
                             country,
                             country_code,
                             description,
                             embedding)
                            SELECT DISTINCT
                            ON (show_id)
                                show_id,
                                title,
                                country,
                                country_code,
                                description,
                                embedding_text::vector(384)
                            FROM public.netflix_titles_silver_staging

                            ON CONFLICT (show_id)
                                DO
                            UPDATE SET
                                title = EXCLUDED.title,
                                country = EXCLUDED.country,
                                country_code = EXCLUDED.country_code,
                                description = EXCLUDED.description,
                                embedding = EXCLUDED.embedding;
                            """)

    finally:
        conn.close()


# The main function --------------------------------------------------------------------------------------------------

def main():
    spark = (
        SparkSession.builder
        .appName("SilverETL")
        .getOrCreate()
    )

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

    df = (
        spark.read.parquet(
            "/app/data/bronze/netflix_titles"
        )
    )

    df.printSchema()

    print(f"======================================================================================================== \n"
          " DATA FROM PARQUET ABOVE \n"
          "===========================================================================================================")

    silver_df = df.dropna(subset=["country", "description"])

    silver_df = silver_df.withColumn(
        "country_code",
        country_to_iso("country")
    )

    silver_df = silver_df.withColumn(
        "embedding",
        embedding_udf("description")
    )

    aggregated_df = (
        silver_df
        .withColumn(
            "country_code", split(col("country_code"), ",")
        )
        .withColumn(
            "country_code", explode(col("country_code")
            )
        )
        .withColumn(
            "country_code", trim(col("country_code")
            )
        )
        .groupBy("country_code")
        .count()
        .orderBy(col("count"), ascending=False)
    )

    aggregated_df.show(100, truncate=False)

    (
        aggregated_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.netflix_titles_aggregated_table")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"======================================================================================================== \n"
          " ^^^ SAVED AGGREGATED DATAFRAME ^^^ \n"
          "===========================================================================================================")

    # Saving embeddings to an intermediate table --------------------------------------------------------------------------

    staging_df = (silver_df.withColumn(
        "embedding_text",
        to_json(col("embedding")),
    ).drop("embedding"))

    (
        staging_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.netflix_titles_silver_staging")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print("Staging table written successfully.")

    load_embeddings_to_pgvector()
    print("pgvector table netflix_titles_silver updated.")

    spark.stop()


if __name__ == "__main__":
    main()
