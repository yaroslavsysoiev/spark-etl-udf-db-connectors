from pyspark.sql.functions import col, current_timestamp

from spark_pipelines.spark_utils.spark_session import create_spark_session
from spark_pipelines.streaming_pipeline.config import (
    BRONZE_DELTA_PATH,
    CHECKPOINT_KAFKA_TO_DELTA,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


def main() -> None:
    spark = create_spark_session("KafkaToDelta")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = raw.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_event"),
    ).withColumn("ingestion_timestamp", current_timestamp())

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_KAFKA_TO_DELTA)
        .start(BRONZE_DELTA_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()