import io
import json
import logging
import time

from kafka import KafkaProducer
from minio import Minio
from minio.commonconfig import CopySource

from spark_pipelines.streaming_pipelines.movie_events_streaming_pipeline.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    MINIO_SECURE,
    PRODUCER_POLL_SECONDS,
)
from spark_pipelines.streaming_pipelines.common.models.event import make_event

logger = logging.getLogger(__name__)

SEED_EVENTS = [
    make_event(101, "s101", "view"),
    make_event(102, "s202", "like"),
    make_event(103, "s303", "pause"),
]


def minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"Created bucket {MINIO_BUCKET}")


def seed_if_empty(client: Minio) -> None:
    inbox = list(client.list_objects(MINIO_BUCKET, prefix="inbox/", recursive=True))
    processed = list(client.list_objects(MINIO_BUCKET, prefix="processed/", recursive=True))
    if inbox or processed:
        return

    for event in SEED_EVENTS:
        payload = event.to_json().encode("utf-8")
        object_name = f"inbox/{event.event_id}.json"
        client.put_object(
            MINIO_BUCKET,
            object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        print(f"Seeded {object_name}")


def parse_payload(raw: bytes) -> list[dict]:
    data = json.loads(raw.decode("utf-8"))
    events = data if isinstance(data, list) else [data]

    for event in events:
        if not isinstance(event, dict):
            raise ValueError("Every event must be a JSON object")

        for field in ("event_id", "movie_id", "event_type", "event_ts"):
            value = event.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Invalid {field}")

        if type(event.get("user_id")) is not int:
            raise ValueError("user_id must be an integer")

        if event["event_type"] not in {"view", "like", "pause"}:
            raise ValueError("Unsupported event_type")

    return events


def main() -> None:
    client = minio_client()
    ensure_bucket(client)
    seed_if_empty(client)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all"
    )

    print(f"Watching s3://{MINIO_BUCKET}/inbox -> kafka://{KAFKA_TOPIC}")

    while True:
        try:
            for obj in client.list_objects(
                    MINIO_BUCKET,
                    prefix="inbox/",
                    recursive=True
            ):

                response = client.get_object(MINIO_BUCKET, obj.object_name)

                try:
                    raw = response.read()
                finally:
                    response.close()
                    response.release_conn()

                try:
                    events = parse_payload(raw)
                except(ValueError, UnicodeDecodeError) as exc:
                    rejected_name = obj.object_name.replace(
                        "inbox/", "rejected/", 1
                    )

                    # copy the rejected file before removing it
                    client.copy_object(
                        MINIO_BUCKET,
                        rejected_name,
                        CopySource(MINIO_BUCKET, obj.object_name),
                    )
                    client.remove_object(MINIO_BUCKET, obj.object_name)

                    print(f"Rejected {obj.object_name}: {exc}")
                    continue


                for event in events:
                    producer.send(
                        KAFKA_TOPIC,
                        key=event["event_id"],
                        value=event,
                    ).get(timeout=60)

                archive_name = obj.object_name.replace("inbox/", "processed/", 1)

                client.copy_object(
                    MINIO_BUCKET,
                    archive_name,
                    CopySource(MINIO_BUCKET, obj.object_name),
                )
                client.remove_object(MINIO_BUCKET, obj.object_name)

        except Exception:
            logger.exception("Producer poll failed")

        time.sleep(PRODUCER_POLL_SECONDS)


if __name__ == "__main__":
    main()