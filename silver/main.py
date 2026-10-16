import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, explode, split, trim

from bronze.config import jdbc_url
from silver.data_io import read_parquet, write_to_staging_table, save_aggregations_to_table

from silver.country_to_iso_udf import country_to_iso
from silver.spark_session import create_spark_session
from silver.support_embedding_udf import embedding_udf, load_embeddings_to_pgvector


# The main function --------------------------------------------------------------------------------------------------
def main():
    spark = create_spark_session()

    df = read_parquet(spark)

    # Data cleansing ---------------------------------------------------------------------------------------------------
    silver_df = df.dropna(subset=["country", "description"])

    # country_to_iso UDF -----------------------------------------------------------------------------------------------
    silver_df = silver_df.withColumn(
        "country_code",
        country_to_iso("country")
    )

    # Aggregations -----------------------------------------------------------------------------------------------------
    aggregated_df = (
        silver_df
        .withColumn(
            "country_code", split(col("country_code"), ",")
        )
        .withColumn(
            "country_code", explode(col("country_code")
                                    )
        )
        .withColumn(
            "country_code", trim(col("country_code")
                                 )
        )
        .groupBy("country_code")
        .count()
        .orderBy(col("count"), ascending=False)
    )

    aggregated_df.show(10, truncate=False)

    save_aggregations_to_table(aggregated_df)

    # Embedding UDF -----------------------------------------------------------------------------------------------
    silver_df = silver_df.withColumn(
        "embedding",
        embedding_udf("description")
    )

    staging_df = (silver_df.withColumn(
        "embedding_text",
        to_json(col("embedding")),
    ).drop("embedding"))

    # Saving embeddings to an intermediate table -----------------------------------------------------------------------
    write_to_staging_table(staging_df)

    # Load data from the staging table into the final Silver table
    load_embeddings_to_pgvector()

    spark.stop()


if __name__ == "__main__":
    main()
