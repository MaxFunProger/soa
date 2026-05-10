"""Простой генератор синтетических складских событий."""
from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone

from .kafka_producer import WMSProducer

log = logging.getLogger(__name__)

ZONES = ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D"]
PRODUCTS = [f"SKU-GEN-{i:03d}" for i in range(1, 11)]


class GeneratorThread(threading.Thread):
    def __init__(self, producer: WMSProducer, events_per_second: float = 1.0) -> None:
        super().__init__(daemon=True, name="wms-generator")
        self._producer = producer
        self._eps = max(events_per_second, 0.01)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def _ts(self) -> int:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    def _produce_received(self) -> None:
        product = random.choice(PRODUCTS)
        zone = random.choice(ZONES)
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._ts(),
            "product_id": product,
            "zone_id": zone,
            "quantity": random.randint(5, 50),
        }
        self._producer.publish("ProductReceived__v1", event, partition_key=product)

    def _produce_received_v2(self) -> None:
        product = random.choice(PRODUCTS)
        zone = random.choice(ZONES)
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._ts(),
            "product_id": product,
            "zone_id": zone,
            "quantity": random.randint(5, 50),
            "supplier_id": f"SUP-{random.randint(1, 5):03d}",
        }
        self._producer.publish("ProductReceived__v2", event, partition_key=product)

    def _produce_shipped(self) -> None:
        product = random.choice(PRODUCTS)
        zone = random.choice(ZONES)
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._ts(),
            "product_id": product,
            "zone_id": zone,
            "quantity": random.randint(1, 5),
        }
        self._producer.publish("ProductShipped", event, partition_key=product)

    def _produce_moved(self) -> None:
        product = random.choice(PRODUCTS)
        z = random.sample(ZONES, 2)
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._ts(),
            "product_id": product,
            "from_zone_id": z[0],
            "to_zone_id": z[1],
            "quantity": random.randint(1, 3),
        }
        self._producer.publish("ProductMoved", event, partition_key=product)

    def run(self) -> None:
        producers = [
            (5, self._produce_received),
            (3, self._produce_received_v2),
            (4, self._produce_shipped),
            (2, self._produce_moved),
        ]
        weights = [w for w, _ in producers]
        funcs = [f for _, f in producers]
        period = 1.0 / self._eps
        log.info("generator started: eps=%.2f period=%.3fs", self._eps, period)
        while not self._stop.is_set():
            try:
                fn = random.choices(funcs, weights=weights, k=1)[0]
                fn()
            except Exception as exc:
                log.exception("generator: failed to publish event: %s", exc)
            self._stop.wait(period)
        log.info("generator stopped")
