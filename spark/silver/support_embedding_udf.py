import os

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

import pandas as pd
import psycopg2


@pandas_udf(ArrayType(FloatType()))
def embedding_udf(texts: pd.Series) -> pd.Series:
    if not hasattr(embedding_udf, "_model"):
        from sentence_transformers import SentenceTransformer

        embedding_udf._model = SentenceTransformer(
            EMBED_MODEL
        )

    vectors = embedding_udf._model.encode(
        texts.tolist(),
        batch_size=32,
    )

    return pd.Series(
        [vector.tolist() for vector in vectors]
    )


EMBED_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


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
