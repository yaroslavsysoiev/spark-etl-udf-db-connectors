import logging

from spark_pipelines.movies_pipeline.config import CSV_PATH, BRONZE_PATH
from spark_pipelines.spark_utils.spark_session import create_spark_session

from spark_pipelines.spark_utils.readers import read_csv
from spark_pipelines.spark_utils.writers import write_to_parquet

logger = logging.getLogger(__name__)


def main():

    logger.info("Starting Bronze ETL")

    spark = create_spark_session("BronzeETL")

    try:
        df = read_csv(spark, CSV_PATH)

        logger.info(
            "Successfully read %s rows from %s",
            df.count(),
            CSV_PATH
        )

        logger.info(
            "Successfully read CSV file from %s",
            CSV_PATH
        )

        logger.info(
            "Writing data to %s",
            BRONZE_PATH
        )

        write_to_parquet(df)

        logger.info(
            "Successfully wrote Bronze parquet files to %s",
            BRONZE_PATH
        )

    finally:

        spark.stop()

    logger.info(
        "Bronze ETL finished successfully"
    )


if __name__ == "__main__":
    main()
