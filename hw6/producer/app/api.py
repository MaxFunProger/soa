"""HTTP API для публикации складских событий."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from .kafka_producer import WMSProducer

log = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _ts_ms(value: datetime | int | None) -> int:
    if value is None:
        return _now_ms()
    if isinstance(value, int):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _ensure_event_id(value: str | None) -> str:
    if value:
        try:
            uuid.UUID(value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="event_id must be UUID") from exc
        return value
    return str(uuid.uuid4())


# ----------------------------- Pydantic-схемы --------------------------------


class ReceivedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)
    supplier_id: str | None = Field(default=None, description="V2: поставщик; null для V1")
    use_v2: bool = Field(default=False, description="Принудительно использовать схему V2")


class ShippedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    quantity: int = Field(description="Количество (отрицательное допустимо для проверки DLQ)")


class MovedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    product_id: str = Field(min_length=1)
    from_zone_id: str = Field(min_length=1)
    to_zone_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)


class ReservedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)
    order_id: str | None = None


class ReleasedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)
    order_id: str | None = None


class CountedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    counted_quantity: int = Field(ge=0)


class OrderItemPayload(BaseModel):
    product_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)


class OrderCreatedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    order_id: str = Field(min_length=1)
    items: list[OrderItemPayload] = Field(min_length=1)


class OrderCompletedRequest(BaseModel):
    event_id: str | None = None
    timestamp: datetime | int | None = None
    order_id: str = Field(min_length=1)


class EventResponse(BaseModel):
    event_id: str
    record_name: str
    schema_version: str | None = None
    status: str = "accepted"


# ----------------------------- App factory -----------------------------------


def build_app(producer: WMSProducer) -> FastAPI:
    """Сборка FastAPI-приложения.

    Для ProductReceived поддерживаются оба варианта схемы: V1 (без supplier_id)
    и V2 (с supplier_id). Выбор делается по полям payload (use_v2 или supplier_id).
    """
    app = FastAPI(title="wms-producer", version="1.0.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/events/product-received", response_model=EventResponse, status_code=202)
    def product_received(payload: ReceivedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)

        use_v2 = payload.use_v2 or payload.supplier_id is not None
        if use_v2:
            event = {
                "event_id": event_id,
                "timestamp": ts,
                "product_id": payload.product_id,
                "zone_id": payload.zone_id,
                "quantity": payload.quantity,
                "supplier_id": payload.supplier_id,
            }
            producer.publish("ProductReceived__v2", event, partition_key=payload.product_id)
            schema_version = "v2"
        else:
            event = {
                "event_id": event_id,
                "timestamp": ts,
                "product_id": payload.product_id,
                "zone_id": payload.zone_id,
                "quantity": payload.quantity,
            }
            producer.publish("ProductReceived__v1", event, partition_key=payload.product_id)
            schema_version = "v1"
        return EventResponse(event_id=event_id, record_name="ProductReceived", schema_version=schema_version)

    @app.post("/events/product-shipped", response_model=EventResponse, status_code=202)
    def product_shipped(payload: ShippedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "product_id": payload.product_id,
            "zone_id": payload.zone_id,
            "quantity": payload.quantity,
        }
        producer.publish("ProductShipped", event, partition_key=payload.product_id)
        return EventResponse(event_id=event_id, record_name="ProductShipped")

    @app.post("/events/product-moved", response_model=EventResponse, status_code=202)
    def product_moved(payload: MovedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "product_id": payload.product_id,
            "from_zone_id": payload.from_zone_id,
            "to_zone_id": payload.to_zone_id,
            "quantity": payload.quantity,
        }
        producer.publish("ProductMoved", event, partition_key=payload.product_id)
        return EventResponse(event_id=event_id, record_name="ProductMoved")

    @app.post("/events/product-reserved", response_model=EventResponse, status_code=202)
    def product_reserved(payload: ReservedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "product_id": payload.product_id,
            "zone_id": payload.zone_id,
            "quantity": payload.quantity,
            "order_id": payload.order_id,
        }
        producer.publish("ProductReserved", event, partition_key=payload.product_id)
        return EventResponse(event_id=event_id, record_name="ProductReserved")

    @app.post("/events/product-released", response_model=EventResponse, status_code=202)
    def product_released(payload: ReleasedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "product_id": payload.product_id,
            "zone_id": payload.zone_id,
            "quantity": payload.quantity,
            "order_id": payload.order_id,
        }
        producer.publish("ProductReleased", event, partition_key=payload.product_id)
        return EventResponse(event_id=event_id, record_name="ProductReleased")

    @app.post("/events/inventory-counted", response_model=EventResponse, status_code=202)
    def inventory_counted(payload: CountedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "product_id": payload.product_id,
            "zone_id": payload.zone_id,
            "counted_quantity": payload.counted_quantity,
        }
        producer.publish("InventoryCounted", event, partition_key=payload.product_id)
        return EventResponse(event_id=event_id, record_name="InventoryCounted")

    @app.post("/events/order-created", response_model=EventResponse, status_code=202)
    def order_created(payload: OrderCreatedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "order_id": payload.order_id,
            "items": [
                {
                    "product_id": item.product_id,
                    "zone_id": item.zone_id,
                    "quantity": item.quantity,
                }
                for item in payload.items
            ],
        }
        producer.publish("OrderCreated", event, partition_key=payload.order_id)
        return EventResponse(event_id=event_id, record_name="OrderCreated")

    @app.post("/events/order-completed", response_model=EventResponse, status_code=202)
    def order_completed(payload: OrderCompletedRequest) -> EventResponse:
        event_id = _ensure_event_id(payload.event_id)
        ts = _ts_ms(payload.timestamp)
        event = {
            "event_id": event_id,
            "timestamp": ts,
            "order_id": payload.order_id,
        }
        producer.publish("OrderCompleted", event, partition_key=payload.order_id)
        return EventResponse(event_id=event_id, record_name="OrderCompleted")

    return app
