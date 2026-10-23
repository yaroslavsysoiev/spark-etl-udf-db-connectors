from pyspark.sql.functions import col, explode, split, trim

from spark_pipelines.spark_utils.spark_session import create_spark_session
from spark_pipelines.spark_utils.readers import read_parquet
from spark_pipelines.spark_utils.writers import jdbc_writer, write_embeddings_to_postgres
from spark_pipelines.spark_utils.udfs.embedding_udf import generate_embeddings
from spark_pipelines.spark_utils.udfs.country_to_iso_udf import country_to_iso



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
    silver_df = silver_df.withColumn(
        "embedding",
        generate_embeddings("description")
    )

    write_embeddings_to_postgres(silver_df)


    spark.stop()


if __name__ == "__main__":
    main()
