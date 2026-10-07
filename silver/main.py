import os

import pandas as pd
import pycountry

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


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

    print("========== DATA FROM POSTGRES ==========")

    df.select(
        "title",
        "country",
        "listed_in",
    ).show(10, truncate=False)

    silver_df = (
        df
        .withColumn(
            "country_code",
            country_to_iso("country")
        )
    )

    print("========== SILVER DATA ==========")

    silver_df.select(
        "title",
        "country",
        "country_code",
        "listed_in",
    ).show(20, truncate=False)

    silver_df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
