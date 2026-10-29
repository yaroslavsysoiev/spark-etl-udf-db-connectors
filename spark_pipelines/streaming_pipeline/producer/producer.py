from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

producer.send(
    "movie-events",
    {
        "event_id": "evt-1",
        "user_id": 123,
        "movie_id": "s101",
        "event_type": "view"
    }
)

producer.flush()
