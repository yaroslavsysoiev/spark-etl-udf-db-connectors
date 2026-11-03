import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "events")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BRONZE_DELTA_PATH = os.getenv(
    "STREAMING_BRONZE_DELTA_PATH",
    "/app/data/streaming/bronze/movie_events",
)
CHECKPOINT_KAFKA_TO_DELTA = os.getenv(
    "CHECKPOINT_KAFKA_TO_DELTA",
    "/app/data/streaming/checkpoints/kafka_to_delta",
)
CHECKPOINT_DELTA_TO_MONGO = os.getenv(
    "CHECKPOINT_DELTA_TO_MONGO",
    "/app/data/streaming/checkpoints/delta_to_mongo",
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "streaming")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "movie_events")

PRODUCER_POLL_SECONDS = int(os.getenv("PRODUCER_POLL_SECONDS", "5"))