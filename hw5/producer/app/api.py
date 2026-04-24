"""HTTP API for accepting and publishing Movie events."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, ValidationError, field_validator

from .kafka_producer import MovieEventProducer

log = logging.getLogger(__name__)

EventType = Literal[
    "VIEW_STARTED",
    "VIEW_FINISHED",
    "VIEW_PAUSED",
    "VIEW_RESUMED",
    "LIKED",
    "SEARCHED",
]
DeviceType = Literal["MOBILE", "DESKTOP", "TV", "TABLET"]


class EventRequest(BaseModel):
    event_id: str | None = Field(default=None, description="Optional pre-assigned UUID")
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime | None = Field(default=None, description="UTC timestamp; defaults to now")
    device_type: DeviceType
    session_id: str = Field(min_length=1)
    progress_seconds: int = Field(default=0, ge=0)

    @field_validator("event_id")
    @classmethod
    def _validate_uuid(cls, value: str | None) -> str | None:
        if value is None:
            return value
        try:
            uuid.UUID(value)
        except ValueError as exc:
            raise ValueError("event_id must be a valid UUID") from exc
        return value


class EventResponse(BaseModel):
    event_id: str
    status: str = "accepted"


def build_app(producer: MovieEventProducer) -> FastAPI:
    app = FastAPI(title="movie-events-producer", version="1.0.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/events", response_model=EventResponse, status_code=202)
    def publish_event(payload: EventRequest) -> EventResponse:
        try:
            event_id = payload.event_id or str(uuid.uuid4())
            ts = payload.timestamp or datetime.now(tz=timezone.utc)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ts_ms = int(ts.timestamp() * 1000)

            event = {
                "event_id": event_id,
                "user_id": payload.user_id,
                "movie_id": payload.movie_id,
                "event_type": payload.event_type,
                "timestamp": ts_ms,
                "device_type": payload.device_type,
                "session_id": payload.session_id,
                "progress_seconds": payload.progress_seconds,
            }
            producer.publish(event)
            return EventResponse(event_id=event_id)
        except ValidationError as exc:
            raise HTTPException(status_code=400, detail=exc.errors()) from exc
        except Exception as exc:
            log.exception("Failed to publish event: %s", exc)
            raise HTTPException(status_code=500, detail="publish_failed") from exc

    return app
