import os
import torch

#Paths
CSV_PATH = "/app/data/raw/netflix_titles.csv"
BRONZE_PATH = "/app/data/bronze/netflix_titles"

# Tables
SILVER_TABLE_NAME = "public.netflix_titles_silver"
AGGREGATED_TABLE_NAME = "public.netflix_titles_aggregated_table"

#Embedding_model
EMBED_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
BATCH_SIZE = 32
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{os.environ['POSTGRES_HOST']}:5432/"
    f"{os.environ['POSTGRES_DB']}"
)

CONNECTION_PROPERTIES = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}
