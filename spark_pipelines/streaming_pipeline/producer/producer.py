import io
import json
import time

from kafka import KafkaProducer
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from spark_pipelines.streaming_pipeline.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    MINIO_SECURE,
    PRODUCER_POLL_SECONDS,
)
from spark_pipelines.streaming_pipeline.models.event import make_event


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
    objects = list(client.list_objects(MINIO_BUCKET, prefix="inbox/", recursive=True))
    if objects:
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
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    raise ValueError("Event file must be a JSON object or array")


def main() -> None:
    client = minio_client()
    ensure_bucket(client)
    seed_if_empty(client)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    processed: set[str] = set()
    print(f"Watching s3://{MINIO_BUCKET}/inbox -> kafka://{KAFKA_TOPIC}")

    while True:
        try:
            for obj in client.list_objects(MINIO_BUCKET, prefix="inbox/", recursive=True):
                if obj.object_name in processed:
                    continue

                response = client.get_object(MINIO_BUCKET, obj.object_name)
                try:
                    events = parse_payload(response.read())
                finally:
                    response.close()
                    response.release_conn()

                for event in events:
                    key = str(event.get("event_id") or obj.object_name)
                    producer.send(KAFKA_TOPIC, key=key, value=event)
                    print(f"Published {key} from {obj.object_name}")

                producer.flush()

                archive_name = obj.object_name.replace("inbox/", "processed/", 1)
                client.copy_object(
                    MINIO_BUCKET,
                    archive_name,
                    CopySource(MINIO_BUCKET, obj.object_name),
                )
                client.remove_object(MINIO_BUCKET, obj.object_name)
                processed.add(obj.object_name)

        except S3Error as exc:
            print(f"MinIO error: {exc}")
        except Exception as exc:
            print(f"Producer error: {exc}")

        time.sleep(PRODUCER_POLL_SECONDS)


if __name__ == "__main__":
    main()