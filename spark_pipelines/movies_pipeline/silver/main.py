from pyspark.sql.functions import to_json, col, explode, split, trim

from spark_pipelines.spark_utils.readers import read_parquet
from spark_pipelines.spark_utils.writers import jdbc_writer

from spark_pipelines.spark_utils.udfs.country_to_iso_udf import country_to_iso
from spark_pipelines.spark_utils.spark_session import create_spark_session
from spark_pipelines.spark_utils.udfs.support_embedding_udf import embedding_udf, load_embeddings_to_pgvector


# The main function --------------------------------------------------------------------------------------------------
def main():
    spark = create_spark_session("SilverETL")

    df = read_parquet(spark)

    # Data cleansing ---------------------------------------------------------------------------------------------------
    silver_df = df.dropna(subset=["country", "description"])

    # country_to_iso UDF -----------------------------------------------------------------------------------------------
    silver_df = silver_df.withColumn(
        "country_code",
        country_to_iso("country")
    )

    # Aggregations -----------------------------------------------------------------------------------------------------
    # #! Try adding persist() before explode() + unpersist
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

    jdbc_writer(
        aggregated_df,
        "public.netflix_titles_aggregated_table"
    ).mode("overwrite").save()

    # Embedding UDF -----------------------------------------------------------------------------------------------
    # #! remove to_json()
    silver_df = silver_df.withColumn(
        "embedding",
        embedding_udf("description")
    )

    staging_df = (silver_df.withColumn(
        "embedding_text",
        to_json(col("embedding")),
    ).drop("embedding"))

    # Saving embeddings to an intermediate table -----------------------------------------------------------------------
    jdbc_writer(
        staging_df,
        "public.netflix_titles_silver_staging"
    ).mode("overwrite").save()

    # Load data from the staging table into the final Silver table
    load_embeddings_to_pgvector()

    spark.stop()


if __name__ == "__main__":
    main()
