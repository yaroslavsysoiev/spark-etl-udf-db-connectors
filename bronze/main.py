from spark_session import create_spark_session
from data_io import (
    read_csv,
    write_to_postgres,
    read_from_postgres,
)


def main():
    spark = create_spark_session()

    df = read_csv(spark)

    print("========== ORIGINAL DATA ==========")
    df.show(5)
    df.printSchema()


    write_to_postgres(df)

    print("Saved to PostgreSQL")

    db_df = read_from_postgres(spark)

    db_df.select(
        "title",
        "country",
        "listed_in",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()