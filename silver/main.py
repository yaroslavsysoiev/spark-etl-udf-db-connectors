import os

import pandas as pd
import pycountry

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, udf, to_json, col
from pyspark.sql.types import StringType, ArrayType, FloatType


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


# My second UDF function ---------------------------------------------------------------------------------------------
# I will switch to pandas_udf in further commits, but I want to test this solution with python UDF for now
from sentence_transformers import \
    SentenceTransformer  # I keep the import here, because I want to split this module into

# different modules while doing chores, but working version is my priority for now

model = SentenceTransformer(
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

print(model.encode("Hello world").shape)

# this print below is to find necessary locks after running `docker compose run --rm silver`
print(f"======================================================================================================== \n"
      " EXAMPLE WITH MODEL \n"
      "===========================================================================================================")


# it will not return vector type, so I would probably need to implement a intermediate table to store ArrayType((FloatType()))
# and then cast it to Vector(384) -- I decided to keep 384 instead of 128 in order not to truncate embeddings
def encode_text(text):
    if text is None:
        return None

    return model.encode(text).tolist()


embedding_udf = udf(
    encode_text,
    ArrayType(FloatType())
)


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
    df.select("show_id", "title", "type", "country", "description").show(25)

    print(f"======================================================================================================== \n"
          " DATA FROM POSTGRES ABOVE \n"
          "===========================================================================================================")

    silver_df = df.dropna()

    silver_df = silver_df.withColumn(
        "country_code",
        country_to_iso("country")
    )

    silver_df = silver_df.withColumn(
        "embedding",
        embedding_udf("description")
    )

    silver_df.select("show_id", "title", "country", "country_code").show(25)
    silver_df.select("show_id", "title", "description", "embedding").show(25)

    print(f"======================================================================================================== \n"
          " ^^^ SILVER DATA ^^^ \n"
          "===========================================================================================================")
    silver_df.printSchema()

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


    spark.stop()


if __name__ == "__main__":
    main()
