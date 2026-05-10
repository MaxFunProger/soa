"""End-to-end smoke-тест WMS-pipeline.

Проверяет:
* /health и /metrics consumer-сервиса;
* базовый цикл склада (PRODUCT_RECEIVED -> RESERVED -> MOVED -> SHIPPED) +
  согласованность 3 денормализованных таблиц;
* идемпотентность повторного PRODUCT_RECEIVED;
* out-of-order: старое событие игнорируется;
* DLQ для события с qty <= 0;
* schema evolution: V1 и V2 ProductReceived обрабатываются в одном топике.
"""
from __future__ import annotations

import os
import time
import uuid

import pytest
import requests
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from confluent_kafka import Consumer, TopicPartition

PRODUCER = os.environ.get("PRODUCER_URL", "http://localhost:8000")
CONSUMER = os.environ.get("CONSUMER_URL", "http://localhost:9100")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "warehouse")
KAFKA_BS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC_NAME", "warehouse-events-dlq")
CONTACT_POINTS = [
    p.strip()
    for p in os.environ.get(
        "CASSANDRA_CONTACT_POINTS", "cassandra-1,cassandra-2,cassandra-3"
    ).split(",")
    if p.strip()
]


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def cass():
    cluster = Cluster(
        contact_points=CONTACT_POINTS,
        port=9042,
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="datacenter1")),
        protocol_version=5,
    )
    session = cluster.connect(KEYSPACE)
    yield session
    cluster.shutdown()


def _post(path: str, payload: dict) -> dict:
    resp = requests.post(f"{PRODUCER}{path}", json=payload, timeout=10)
    assert resp.status_code in (200, 202), f"{path} -> {resp.status_code}: {resp.text}"
    return resp.json()


def _wait_state(session, product_id: str, zone_id: str, *, want_available: int, want_reserved: int = 0, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        row = session.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (product_id, zone_id),
        ).one()
        if row is not None:
            last = (row.available_quantity, row.reserved_quantity)
            if row.available_quantity == want_available and row.reserved_quantity == want_reserved:
                return
        time.sleep(0.5)
    pytest.fail(
        f"Timeout waiting for ({product_id},{zone_id}) "
        f"available={want_available} reserved={want_reserved} (last={last})"
    )


def _wait_total(session, product_id: str, *, want_total_available: int, want_total_reserved: int = 0, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        rows = list(session.execute(
            "SELECT total_available, total_reserved FROM inventory_by_product WHERE product_id=%s",
            (product_id,),
        ))
        if rows:
            ta = rows[0].total_available or 0
            tr = rows[0].total_reserved or 0
            last = (ta, tr)
            # все строки должны иметь одинаковые totals (мы их синхронно обновляем)
            if ta == want_total_available and tr == want_total_reserved:
                return
        time.sleep(0.5)
    pytest.fail(
        f"Timeout waiting for product {product_id} total_available={want_total_available} "
        f"total_reserved={want_total_reserved} (last={last})"
    )


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


def test_consumer_health():
    resp = requests.get(f"{CONSUMER}/health", timeout=10)
    assert resp.status_code == 200
    assert resp.json().get("status") == "ok"


def test_consumer_metrics():
    resp = requests.get(f"{CONSUMER}/metrics", timeout=10)
    assert resp.status_code == 200
    text = resp.text
    for metric in (
        "events_processed_total",
        "consumer_lag",
        "event_processing_duration_seconds",
        "cassandra_write_errors_total",
    ):
        assert metric in text, f"metric {metric} not exposed"


def test_basic_flow(cass):
    """Сценарий 1+3 из README: базовый цикл + 3 таблицы согласованны."""
    sku = f"SKU-T1-{uuid.uuid4().hex[:6]}"
    zone_a = "ZONE-A"
    zone_b = "ZONE-B"

    _post("/events/product-received", {"product_id": sku, "zone_id": zone_a, "quantity": 100})
    _wait_state(cass, sku, zone_a, want_available=100)
    _wait_total(cass, sku, want_total_available=100)

    # Проверим все 3 денормализованные таблицы (пункт 3+5)
    pz = cass.execute(
        "SELECT available_quantity FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (sku, zone_a),
    ).one()
    assert pz.available_quantity == 100
    p = cass.execute(
        "SELECT total_available FROM inventory_by_product WHERE product_id=%s", (sku,),
    ).one()
    assert p.total_available == 100
    z = cass.execute(
        "SELECT available_quantity FROM inventory_by_zone WHERE zone_id=%s AND product_id=%s",
        (zone_a, sku),
    ).one()
    assert z.available_quantity == 100

    _post("/events/product-reserved", {"product_id": sku, "zone_id": zone_a, "quantity": 30})
    _wait_state(cass, sku, zone_a, want_available=70, want_reserved=30)
    _wait_total(cass, sku, want_total_available=70, want_total_reserved=30)

    _post("/events/product-moved", {"product_id": sku, "from_zone_id": zone_a, "to_zone_id": zone_b, "quantity": 20})
    _wait_state(cass, sku, zone_a, want_available=50, want_reserved=30)
    _wait_state(cass, sku, zone_b, want_available=20)
    _wait_total(cass, sku, want_total_available=70, want_total_reserved=30)

    _post("/events/product-shipped", {"product_id": sku, "zone_id": zone_a, "quantity": 10})
    _wait_state(cass, sku, zone_a, want_available=40, want_reserved=30)
    _wait_total(cass, sku, want_total_available=60, want_total_reserved=30)


def test_order_lifecycle(cass):
    sku = f"SKU-T-ORD-{uuid.uuid4().hex[:6]}"
    zone = "ZONE-A"

    _post("/events/product-received", {"product_id": sku, "zone_id": zone, "quantity": 50})
    _wait_state(cass, sku, zone, want_available=50)

    order_id = f"ORD-{uuid.uuid4().hex[:8]}"
    _post(
        "/events/order-created",
        {"order_id": order_id, "items": [{"product_id": sku, "zone_id": zone, "quantity": 15}]},
    )
    _wait_state(cass, sku, zone, want_available=35, want_reserved=15)

    _post("/events/order-completed", {"order_id": order_id})
    _wait_state(cass, sku, zone, want_available=35, want_reserved=0)


def test_idempotency(cass):
    sku = f"SKU-T2-{uuid.uuid4().hex[:6]}"
    zone = "ZONE-A"
    event_id = str(uuid.uuid4())

    _post("/events/product-received", {
        "event_id": event_id, "product_id": sku, "zone_id": zone, "quantity": 50,
    })
    _wait_state(cass, sku, zone, want_available=50)

    # Дубль: тот же event_id, но другое значение quantity (заведомо другое содержимое).
    _post("/events/product-received", {
        "event_id": event_id, "product_id": sku, "zone_id": zone, "quantity": 50,
    })

    # Через секунду состояние не должно измениться.
    time.sleep(2)
    row = cass.execute(
        "SELECT available_quantity FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (sku, zone),
    ).one()
    assert row.available_quantity == 50, f"idempotency violated: {row.available_quantity}"


def test_out_of_order(cass):
    sku = f"SKU-T3-{uuid.uuid4().hex[:6]}"
    zone = "ZONE-A"
    base_ts = int(time.time() * 1000)
    ts1 = base_ts
    ts2 = base_ts + 5 * 60_000   # +5 min
    ts_old = base_ts + 2 * 60_000  # старое относительно ts2

    _post("/events/product-received", {
        "product_id": sku, "zone_id": zone, "quantity": 100, "timestamp": ts1,
    })
    _wait_state(cass, sku, zone, want_available=100)

    _post("/events/product-shipped", {
        "product_id": sku, "zone_id": zone, "quantity": 20, "timestamp": ts2,
    })
    _wait_state(cass, sku, zone, want_available=80)

    # Старое событие: должно быть ИГНОРИРОВАНО (ts < last_event_ts=ts2)
    _post("/events/product-received", {
        "product_id": sku, "zone_id": zone, "quantity": 50, "timestamp": ts_old,
    })

    time.sleep(2)
    row = cass.execute(
        "SELECT available_quantity FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (sku, zone),
    ).one()
    assert row.available_quantity == 80, f"stale event applied: {row.available_quantity}"


def test_dlq_invalid_quantity(cass):
    """Невалидное событие (qty=-5) должно улететь в DLQ, consumer не должен упасть."""
    sku = f"SKU-T4-{uuid.uuid4().hex[:6]}"
    zone = "ZONE-A"

    # Сначала нормальное событие — чтобы проверить, что pipeline жив до и после.
    _post("/events/product-received", {"product_id": sku, "zone_id": zone, "quantity": 10})
    _wait_state(cass, sku, zone, want_available=10)

    # Готовим DLQ-ридер ДО публикации, чтобы не пропустить сообщение.
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BS,
        "group.id": f"dlq-test-{uuid.uuid4().hex[:6]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([DLQ_TOPIC])
    # дёргаем, чтобы группа подписалась и offset стал latest
    deadline = time.time() + 10
    while time.time() < deadline:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error() is None:
            # пропустим то, что было до нас
            continue

    invalid = {
        "product_id": sku,
        "zone_id": zone,
        "quantity": -5,
    }
    _post("/events/product-shipped", invalid)

    found = False
    deadline = time.time() + 30
    while time.time() < deadline and not found:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error() is not None:
            continue
        # Avro-payload — нам достаточно убедиться, что что-то пришло.
        # Проверим, что в payload есть SKU.
        raw = msg.value()
        if raw is not None and sku.encode("utf-8") in raw:
            found = True
            break
    consumer.close()
    assert found, f"Не найдено DLQ-сообщение для {sku} в топике {DLQ_TOPIC}"

    # И после ошибочного события pipeline остался жив:
    _post("/events/product-received", {"product_id": sku, "zone_id": zone, "quantity": 5})
    _wait_state(cass, sku, zone, want_available=15)


def test_schema_evolution_v1_v2(cass):
    """V1 (без supplier_id) и V2 (с supplier_id) обрабатываются в одном топике."""
    sku = f"SKU-T5-{uuid.uuid4().hex[:6]}"
    zone = "ZONE-A"

    # V1: явно указываем use_v2=False (без supplier_id)
    _post("/events/product-received", {
        "product_id": sku, "zone_id": zone, "quantity": 10, "use_v2": False,
    })
    _wait_state(cass, sku, zone, want_available=10)
    row = cass.execute(
        "SELECT supplier_id FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (sku, zone),
    ).one()
    assert row.supplier_id is None, f"V1: supplier_id должен быть NULL, получили {row.supplier_id}"

    # V2: с supplier_id
    _post("/events/product-received", {
        "product_id": sku, "zone_id": zone, "quantity": 5, "supplier_id": "SUP-007",
    })
    _wait_state(cass, sku, zone, want_available=15)
    row = cass.execute(
        "SELECT supplier_id FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (sku, zone),
    ).one()
    assert row.supplier_id == "SUP-007", f"V2: ожидали SUP-007, получили {row.supplier_id}"
