from spark_pipelines.spark_utils.spark_session import create_spark_session

from spark_pipelines.spark_utils.data_io import (
    read_csv,
    write_to_parquet,
)


def main():
    spark = create_spark_session("BronzeETL")

    df = read_csv(spark)

    print("========== ORIGINAL DATA ==========")
    df.show(5)
    df.printSchema()


    write_to_parquet(df)

    print("DataFrame was successfully written to parquet")

    spark.stop()


if __name__ == "__main__":
    main()