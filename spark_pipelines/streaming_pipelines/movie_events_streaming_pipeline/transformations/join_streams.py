from pyspark.sql.functions import expr

from spark_pipelines.spark_utils.spark_session import create_spark_session


spark = create_spark_session(
    "SparkTransform"
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from pyspark.sql.functions import col

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("movie_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
])

# Stream A
stream_a = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

stream_a = events_a.select(
    col("event_id"),
    current_timestamp().alias("ts")
)

stream_a = stream_a.withWatermark("ts", "10 minutes")

# Stream B
stream_b = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
stream_b = stream_b.select(expr("CAST(value AS INT) AS id"), expr("current_timestamp() AS ts"))

# Add watermarks
stream_a = stream_a.withWatermark("ts", "10 minutes")
stream_b = stream_b.withWatermark("ts", "10 minutes")

# Join with time condition
joined_streams = stream_a.join(
    stream_b,
    expr("""
        stream_a.id = stream_b.id AND
        stream_a.ts >= stream_b.ts - interval 5 minutes AND
        stream_a.ts <= stream_b.ts + interval 5 minutes
    """),
    how="inner"
)

query = joined_streams.writeStream.outputMode("append").format("console").start()
query.awaitTermination()