import io
import time

from minio import Minio
from spark_pipelines.streaming_pipelines.common.models.event import make_event
from spark_pipelines.streaming_pipelines.movie_events_streaming_pipeline.config import (
    MINIO_ENDPOINT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_SECURE,
    MINIO_BUCKET
)

def main():

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_SECURE,
    )

    counter = 1

    while True:
        event = make_event(
            user_id=counter,
            movie_id=f"s{counter}",
            event_type="view"
        )

        payload = event.to_json().encode("utf-8")

        object_name = f"inbox/{event.event_id}.json"

        client.put_object(
            MINIO_BUCKET,
            object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )

        print(f"Created {object_name}")

        counter += 1

        time.sleep(5)

if __name__ == '__main__':
    main()