from spark_pipelines.movies_pipeline.bronze.spark_session import create_spark_session

from spark_pipelines.movies_pipeline.bronze.data_io import (
    read_csv,
    write_to_parquet,
)


def main():
    spark = create_spark_session()

    df = read_csv(spark)

    print("========== ORIGINAL DATA ==========")
    df.show(5)
    df.printSchema()


    write_to_parquet(df)

    print("DataFrame was successfully written to parquet")

    spark.stop()


if __name__ == "__main__":
    main()