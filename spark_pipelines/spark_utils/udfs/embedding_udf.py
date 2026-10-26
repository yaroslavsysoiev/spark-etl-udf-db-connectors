from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

from spark_pipelines.movies_pipeline.config import EMBED_MODEL

import pandas as pd
import psycopg2
from pgvector.psycopg2 import register_vector

@pandas_udf(ArrayType(FloatType()))
def generate_embeddings(texts: pd.Series) -> pd.Series:
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(
        EMBED_MODEL
    )

    vectors = model.encode(
        texts.tolist(),
        batch_size=32,
    )

    return pd.Series(
        [vector.tolist() for vector in vectors]
    )
