import logging

from pyspark import StorageLevel
from pyspark.sql.functions import col, explode, split, trim
from pyspark.sql.functions import current_timestamp

from spark_pipelines.movies_pipeline.config import AGGREGATED_TABLE_NAME
from spark_pipelines.spark_utils.spark_session import create_spark_session
from spark_pipelines.spark_utils.readers import read_delta
from spark_pipelines.spark_utils.writers import jdbc_writer, write_embeddings_to_postgres

from spark_pipelines.spark_utils.udfs.embedding_udf import generate_embeddings
from spark_pipelines.spark_utils.udfs.country_to_iso_udf import country_to_iso


logger = logging.getLogger(__name__)

def main():

    spark = create_spark_session("SilverETL")

    df = read_delta(spark)

    silver_df = (
        df
        .dropna(subset=["country", "description"])
        .withColumn(
        "country_code",
        country_to_iso("country")
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp()
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
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

        logger.info("Writing aggregated DataFrame to PostgreSQL")

        jdbc_writer(
            aggregated_df,
            AGGREGATED_TABLE_NAME
        ).mode("overwrite").save()

        logger.info("Generating embeddings")


        silver_df = silver_df.withColumn(
            "embedding",
            generate_embeddings("description")
        )

        logger.info("Writing embeddings to PostgreSQL")

        write_embeddings_to_postgres(silver_df)

    finally:
        if "silver_df" in locals():
            silver_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
