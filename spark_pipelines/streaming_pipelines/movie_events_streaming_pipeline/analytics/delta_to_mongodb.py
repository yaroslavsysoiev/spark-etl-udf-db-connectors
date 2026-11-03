import json
import os
import time

from pymongo import MongoClient, UpdateOne
from spark_pipelines.spark_utils.spark_session import create_spark_session
from spark_pipelines.streaming_pipelines.movie_events_streaming_pipeline.config import (
    BRONZE_DELTA_PATH,
    CHECKPOINT_DELTA_TO_MONGO,
    MONGO_COLLECTION,
    MONGO_DB,
    MONGO_URI,
)


def write_unstructured_to_mongo(batch_df, batch_id: int) -> None:
    rows = batch_df.select("topic", "partition", "offset", "raw_event", "kafka_timestamp" "ingestion_timestamp").toLocalIterator()

    with MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) as client:
        database = client[MONGO_DB]
        events = database[MONGO_COLLECTION]
        rejected = database[f"{MONGO_COLLECTION}_rejected"]

        event_operations = []
        rejected_operations = []

        for row in rows:
            source_id = f"{row.topic}:{row.partition}:{row.offset}"

            try:
                payload = json.loads(row.raw_event)
                if not isinstance(payload, dict):
                    raise ValueError("Event must be a JSON object")

                event_id = payload.get("event_id")
                if not isinstance(event_id, str) or not event_id.strip():
                    raise ValueError("Missing or invalid event_id")

            except (TypeError, ValueError) as exc:
                rejected_operations.append(
                    UpdateOne(
                        {"_id": source_id},
                        {
                            "$setOnInsert": {
                                "raw_event": row.raw_event,
                                "error": str(exc),
                            }
                        },
                        upsert=True,
                    )
                )
            else:
                event_operations.append(
                    UpdateOne(
                        {"_id": event_id},
                        {
                            "$setOnInsert": {
                                "event": payload,
                                "source": {
                                    "topic": row.topic,
                                    "partition": row.partition,
                                    "offset": row.offset,
                                },
                                "kafka_timestamp": row.kafka_timestamp,
                                "ingestion_timestamp": row.ingestion_timestamp,
                                "spark_batch_id": batch_id,
                            }
                        },
                        upsert=True,
                    )
                )

            if len(event_operations) >= 500:
                events.bulk_write(event_operations, ordered=True)
                event_operations.clear()

            if len(rejected_operations) >= 500:
                rejected.bulk_write(rejected_operations, ordered=True)
                rejected_operations.clear()

        if event_operations:
            events.bulk_write(event_operations, ordered=True)

        if rejected_operations:
            rejected.bulk_write(rejected_operations, ordered=True)


def wait_for_delta_table(path: str) -> None:
    delta_log = os.path.join(path, "_delta_log")
    while not os.path.isdir(delta_log):
        print(f"Waiting for Delta table at {path}")
        time.sleep(5)


def main() -> None:
    wait_for_delta_table(BRONZE_DELTA_PATH)
    spark = create_spark_session("DeltaToMongo")

    bronze = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(BRONZE_DELTA_PATH)
    )

    query = (
        bronze.writeStream.foreachBatch(write_unstructured_to_mongo)
        .option("checkpointLocation", CHECKPOINT_DELTA_TO_MONGO)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()