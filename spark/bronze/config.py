import os

CSV_PATH = "/app/data/raw/netflix_titles.csv"
TABLE_NAME = "/app/data/bronze/netflix_titles"

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