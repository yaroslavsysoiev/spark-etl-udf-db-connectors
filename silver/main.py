import os

import pandas as pd
import pycountry

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, udf, to_json, col
from pyspark.sql.types import StringType, ArrayType, FloatType

import psycopg2


# My first UDF function using pandas_udf -------------------------------------------------------------------------------
@pandas_udf(StringType())
def country_to_iso(countries: pd.Series) -> pd.Series:
    def convert(value):
        if pd.isna(value):
            return None

        result = []

        for country_name in value.split(","):
            country_name = country_name.strip()

            try:
                country = pycountry.countries.lookup(country_name)
                result.append(country.alpha_2)

            except LookupError:
                result.append("UNKNOWN")

        return ",".join(result)

    return countries.apply(convert)


# the second pandas_udf
# Conclusion from using regular udf: I encountered OOM

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


# Finally writing the embedded description to a new table netflix_titles_silver

def load_embeddings_to_pgvector():
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn:  # one transaction: commits on success, rolls back on error
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

                cur.execute("""
                            CREATE TABLE IF NOT EXISTS public.netflix_titles_silver
                            (
                                show_id
                                text
                                PRIMARY
                                KEY,
                                title
                                text,
                                country
                                text,
                                country_code
                                text,
                                description
                                text,
                                embedding
                                vector
                            (
                                384
                            )
                                );
                            """)

                cur.execute("""
                            INSERT INTO public.netflix_titles_silver
                                (show_id, title, country, country_code, description, embedding)
                            SELECT DISTINCT
                            ON (show_id)
                                show_id, title, country, country_code, description,
                                embedding_text::vector(384)
                            FROM public.netflix_titles_silver_staging
                            ON CONFLICT (show_id) DO
                            UPDATE SET
                                title = EXCLUDED.title,
                                country = EXCLUDED.country,
                                country_code = EXCLUDED.country_code,
                                description = EXCLUDED.description,
                                embedding = EXCLUDED.embedding;
                            """)

                # Optional: similarity-search index (cosine distance)
                cur.execute("""
                            CREATE INDEX IF NOT EXISTS netflix_titles_silver_embedding_idx
                                ON public.netflix_titles_silver
                                USING hnsw (embedding vector_cosine_ops);
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
        spark.read
        .jdbc(
            url=jdbc_url,
            table="public.netflix_titles",
            properties=connection_properties,
        )
    )

    df.printSchema()
    # df.select("show_id", "title", "type", "country", "description").show(25)

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

    # silver_df.select("show_id", "title", "country", "country_code").show(25)
    # silver_df.select("show_id", "title", "description", "embedding").show(25)

    print(f"======================================================================================================== \n"
          " ^^^ SILVER DATA ^^^ \n"
          "===========================================================================================================")
    # silver_df.printSchema()

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
