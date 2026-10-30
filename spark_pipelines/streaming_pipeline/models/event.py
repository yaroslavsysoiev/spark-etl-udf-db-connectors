from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import uuid


@dataclass
class MovieEvent:
    event_id: str
    user_id: int
    movie_id: str
    event_type: str
    event_ts: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def make_event(user_id: int, movie_id: str, event_type: str) -> MovieEvent:
    return MovieEvent(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        movie_id=movie_id,
        event_type=event_type,
        event_ts=datetime.now(timezone.utc).isoformat(),
    )