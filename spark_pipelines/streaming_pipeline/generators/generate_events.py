import io
import json
import time
import uuid

from minio import Minio

from spark_pipelines.streaming_pipeline.config import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_SECURE,
)

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=MINIO_SECURE,
)

BUCKET = "events"

counter = 1

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": counter,
        "movie_id": f"s{counter}",
        "event_type": "view",
    }

    payload = json.dumps(event).encode()

    object_name = f"inbox/{event['event_id']}.json"

    client.put_object(
        BUCKET,
        object_name,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    print(f"Created {object_name}")

    counter += 1

    time.sleep(5)