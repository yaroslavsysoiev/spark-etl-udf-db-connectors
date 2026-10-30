git diffimport json
import os
import time

from pymongo import MongoClient
from spark_pipelines.spark_utils.spark_session import create_spark_session
from spark_pipelines.streaming_pipeline.config import (
    BRONZE_DELTA_PATH,
    CHECKPOINT_DELTA_TO_MONGO,
    MONGO_COLLECTION,
    MONGO_DB,
    MONGO_URI,
)


def write_unstructured_to_mongo(batch_df, batch_id: int) -> None:
    rows = batch_df.select("raw_event", "event_key", "kafka_timestamp", "ingestion_timestamp").collect()
    if not rows:
        return

    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        docs = []
        for row in rows:
            try:
                payload = json.loads(row.raw_event) if row.raw_event else {}
                if not isinstance(payload, dict):
                    payload = {"value": payload}
            except (TypeError, json.JSONDecodeError):
                payload = {
                    "raw_event": row.raw_event,
                    "parse_error": True,
                }

            payload["_kafka_key"] = row.event_key
            payload["_kafka_timestamp"] = (
                row.kafka_timestamp.isoformat() if row.kafka_timestamp else None
            )
            payload["_ingestion_timestamp"] = (
                row.ingestion_timestamp.isoformat() if row.ingestion_timestamp else None
            )
            payload["_spark_batch_id"] = batch_id
            docs.append(payload)

        if docs:
            result = collection.insert_many(docs)
            print(f"Mongo batch {batch_id}: inserted {len(result.inserted_ids)} docs")
    finally:
        client.close()


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