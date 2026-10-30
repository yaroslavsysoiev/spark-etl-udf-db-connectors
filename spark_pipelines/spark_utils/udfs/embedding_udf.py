import logging
import pandas as pd

from typing import Iterator

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

from spark_pipelines.movies_pipeline.config import EMBED_MODEL, BATCH_SIZE, DEVICE


logger = logging.getLogger(__name__)


@pandas_udf(ArrayType(FloatType()))
def generate_embeddings(
        iterator: Iterator[pd.Series]
) -> Iterator[pd.Series]:
    from sentence_transformers import SentenceTransformer
    import numpy as np

    model = SentenceTransformer(
        EMBED_MODEL,
        device=DEVICE
    )

    # Set model to evaluation mode
    model.eval()

    for texts_batch in iterator:

        if len(texts_batch) == 0:
            yield pd.Series([], dtype=object)
            continue

        texts_list = texts_batch.tolist()

        all_embeddings = []

        for i in range(0, len(texts_list), BATCH_SIZE):
            sub_batch = texts_list[i : i + BATCH_SIZE]

            embeddings = model.encode(
                sub_batch,
                batch_size=BATCH_SIZE,
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
                device=DEVICE,
            )


            for idx, embedding in enumerate(embeddings):
                if np.isnan(embedding).any() or np.isinf(embedding).any():
                    text = sub_batch[idx] if idx < len(sub_batch) else "unknown"
                    logger.warning(
                        f"Invalid embedding detected (NaN/inf) for text: '{text[:100]}...'"
                        f" - Setting to None"
                    )
                    all_embeddings.append(None)
                else:
                    all_embeddings.append(embedding.tolist())

        yield pd.Series(all_embeddings)
