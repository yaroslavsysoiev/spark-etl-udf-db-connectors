import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, to_json, col
from pyspark.sql.types import ArrayType, FloatType

import pandas as pd
import psycopg2

from country_to_iso_udf import country_to_iso

# the second pandas_udf -> to be moved to a different module -----------------------------------------------------------

EMBED_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_model = None


def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(EMBED_MODEL)
    return _model


@pandas_udf(ArrayType(FloatType()))
def embedding_udf(texts: pd.Series) -> pd.Series:
    vectors = get_model().encode(texts.tolist(), batch_size=32)
    return pd.Series([v.tolist() for v in vectors])


# Finally writing the embedded description to a new table netflix_titles_silver ----------------------------------------
def load_embeddings_to_pgvector():
    # Establish a connection to the PostgreSQL database.
    # Database credentials are retrieved from environment variables.
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],  # PostgreSQL server hostname
        port=5432,  # Default PostgreSQL port
        dbname=os.environ["POSTGRES_DB"],  # Database name
        user=os.environ["POSTGRES_USER"],  # Database username
        password=os.environ["POSTGRES_PASSWORD"],  # Database password
    )

    try:
        # Start a database transaction.
        # If all operations succeed, changes are committed.
        # If an exception occurs, all changes are rolled back.
        with conn:

            # Create a cursor to execute SQL queries in PostgreSQL.
            # The cursor is automatically closed after leaving this block.
            with conn.cursor() as cur:
                # Enable the pgvector extension if it is not already enabled.
                # This allows PostgreSQL to store and compare vector embeddings.
                cur.execute("""
                            CREATE
                            EXTENSION IF NOT EXISTS vector;
                            """)

                # Create the final Silver table if it does not exist.
                # This table stores Netflix metadata and vector embeddings.
                cur.execute("""
                            CREATE TABLE IF NOT EXISTS public.netflix_titles_silver
                            (
                                show_id
                                TEXT
                                PRIMARY
                                KEY,
                                title
                                TEXT,
                                country
                                TEXT,
                                country_code
                                TEXT,
                                description
                                TEXT,
                                embedding
                                VECTOR
                            (
                                384
                            )
                                );
                            """)

                # Load data from the staging table into the Silver table.
                # DISTINCT ON keeps only one record for each show_id.
                # Convert embedding_text into a 384-dimensional vector.
                # If show_id already exists, update the existing record.
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

                # Create an HNSW index for faster vector similarity searches.
                # vector_cosine_ops enables indexing based on cosine distance.
                # The index is created only if it does not already exist.
                cur.execute("""
                            CREATE INDEX IF NOT EXISTS
                                netflix_titles_silver_embedding_idx
                                ON public.netflix_titles_silver
                                USING hnsw (embedding vector_cosine_ops);
                            """)

    finally:
        # Always close the database connection,
        # even if an exception occurs during execution.
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
        spark.read
        .jdbc(
            url=jdbc_url,
            table="public.netflix_titles",
            properties=connection_properties,
        )
    )

    df.printSchema()

    print(f"======================================================================================================== \n"
          " DATA FROM POSTGRES ABOVE \n"
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

    print(f"======================================================================================================== \n"
          " ^^^ SILVER DATA ^^^ \n"
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
