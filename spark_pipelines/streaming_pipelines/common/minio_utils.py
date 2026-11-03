import logging
import time

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


def ensure_bucket_ready(
    client: Minio,
    bucket: str,
    timeout: float = 60,
) -> None:
    deadline = time.monotonic() + timeout

    while True:
        try:
            if not client.bucket_exists(bucket):
                try:
                    client.make_bucket(bucket)
                except S3Error as exc:
                    # Another process may have created it after our check.
                    if exc.code != "BucketAlreadyOwnedByYou":
                        raise

            return

        except Exception:
            if time.monotonic() >= deadline:
                raise

            logger.warning(
                "Bucket %s is not ready; retrying",
                bucket,
                exc_info=True,
            )
            time.sleep(2)