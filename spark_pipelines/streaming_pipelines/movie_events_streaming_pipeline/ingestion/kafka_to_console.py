from pyspark.sql.functions import col

from spark_pipelines.spark_utils.spark_session import create_spark_session

spark = create_spark_session(
    "KafkaToConsole"
)

df = (
    spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "kafka:9092"
        )
        .option(
            "subscribe",
            "movie-events"
        )
        .load()
)

# kafka value -> string
events_df = (
    df.select(
        col("value")
        .cast("string")
        .alias("event")
    )
)

query = (
    events_df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()
