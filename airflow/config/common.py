import os

POSTGRES_ENV = {
    "POSTGRES_HOST": os.environ["POSTGRES_HOST"],
    "POSTGRES_DB": os.environ["POSTGRES_DB"],
    "POSTGRES_USER": os.environ["POSTGRES_USER"],
    "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
}