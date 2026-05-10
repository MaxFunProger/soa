"""Слой работы с Cassandra: подготовленные запросы, batch-обновления, идемпотентность."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType, PreparedStatement

from .config import Settings

log = logging.getLogger(__name__)

_CL_MAP = {
    "ANY": ConsistencyLevel.ANY,
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
}


def _cl(name: str) -> int:
    return _CL_MAP[name.upper()]


class CassandraStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._cluster: Cluster | None = None
        self._session: Session | None = None
        self._write_cl = _cl(settings.cassandra_write_consistency)
        self._read_cl = _cl(settings.cassandra_read_consistency)
        self._stmt: dict[str, PreparedStatement] = {}

    # ----------------------- Подключение и подготовка ------------------------

    def connect(self, retries: int = 60, backoff_s: float = 5.0) -> None:
        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                self._cluster = Cluster(
                    contact_points=list(self._settings.cassandra_contact_points),
                    port=self._settings.cassandra_port,
                    load_balancing_policy=TokenAwarePolicy(
                        DCAwareRoundRobinPolicy(local_dc=self._settings.cassandra_local_dc),
                    ),
                    protocol_version=5,
                    connect_timeout=10,
                )
                self._session = self._cluster.connect(self._settings.cassandra_keyspace)
                self._session.default_consistency_level = self._write_cl
                self._prepare_statements()
                log.info(
                    "Connected to Cassandra %s/%s (write_cl=%s read_cl=%s)",
                    self._settings.cassandra_contact_points,
                    self._settings.cassandra_keyspace,
                    self._settings.cassandra_write_consistency,
                    self._settings.cassandra_read_consistency,
                )
                return
            except Exception as exc:
                last_err = exc
                log.warning("Cassandra connect attempt %d/%d failed: %s", attempt, retries, exc)
                self._safe_shutdown()
                time.sleep(backoff_s)
        raise RuntimeError(f"Cassandra unreachable: {last_err}")

    def _safe_shutdown(self) -> None:
        try:
            if self._session is not None:
                self._session.shutdown()
        except Exception:
            pass
        try:
            if self._cluster is not None:
                self._cluster.shutdown()
        except Exception:
            pass
        self._session = None
        self._cluster = None

    def shutdown(self) -> None:
        self._safe_shutdown()

    def is_alive(self) -> bool:
        try:
            assert self._session is not None
            row = self._session.execute("SELECT now() FROM system.local").one()
            return row is not None
        except Exception as exc:
            log.warning("Cassandra health check failed: %s", exc)
            return False

    # ----------------------- Prepared statements -----------------------------

    def _prepare_statements(self) -> None:
        s = self._session
        assert s is not None

        self._stmt["select_pz"] = s.prepare(
            "SELECT available_quantity, reserved_quantity, last_event_ts, supplier_id "
            "FROM inventory_by_product_zone WHERE product_id=? AND zone_id=?"
        )
        self._stmt["select_p_breakdown"] = s.prepare(
            "SELECT zone_id, available_quantity, reserved_quantity "
            "FROM inventory_by_product WHERE product_id=?"
        )
        self._stmt["upd_pz"] = s.prepare(
            "UPDATE inventory_by_product_zone "
            "SET available_quantity=?, reserved_quantity=?, last_event_ts=?, supplier_id=? "
            "WHERE product_id=? AND zone_id=?"
        )
        self._stmt["upd_p"] = s.prepare(
            "UPDATE inventory_by_product "
            "SET available_quantity=?, reserved_quantity=?, total_available=?, total_reserved=?, last_event_ts=? "
            "WHERE product_id=? AND zone_id=?"
        )
        self._stmt["upd_z"] = s.prepare(
            "UPDATE inventory_by_zone "
            "SET available_quantity=?, reserved_quantity=?, last_event_ts=? "
            "WHERE zone_id=? AND product_id=?"
        )
        self._stmt["ins_processed"] = s.prepare(
            "INSERT INTO processed_events (event_id, event_type, processed_at, kafka_partition, kafka_offset) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        self._stmt["select_processed"] = s.prepare(
            "SELECT event_id FROM processed_events WHERE event_id=?"
        )
        self._stmt["select_order"] = s.prepare(
            "SELECT order_id, status, items, last_event_ts FROM orders WHERE order_id=?"
        )
        self._stmt["upsert_order"] = s.prepare(
            "UPDATE orders SET status=?, items=?, created_at=?, updated_at=?, last_event_ts=? "
            "WHERE order_id=?"
        )
        self._stmt["ins_event_log"] = s.prepare(
            "INSERT INTO events_log (bucket, event_ts, event_id, event_type, payload) "
            "VALUES (?, ?, ?, ?, ?)"
        )

    # ----------------------- High-level helpers ------------------------------

    def is_already_processed(self, event_id: str) -> bool:
        stmt = self._stmt["select_processed"].bind((event_id,))
        stmt.consistency_level = self._read_cl
        row = self._session.execute(stmt).one()
        return row is not None

    def fetch_pz(self, product_id: str, zone_id: str) -> dict:
        """Текущее состояние (available, reserved, last_event_ts, supplier_id)."""
        stmt = self._stmt["select_pz"].bind((product_id, zone_id))
        stmt.consistency_level = self._read_cl
        row = self._session.execute(stmt).one()
        if row is None:
            return {
                "available_quantity": 0,
                "reserved_quantity": 0,
                "last_event_ts": 0,
                "supplier_id": None,
            }
        return {
            "available_quantity": row.available_quantity or 0,
            "reserved_quantity": row.reserved_quantity or 0,
            "last_event_ts": row.last_event_ts or 0,
            "supplier_id": row.supplier_id,
        }

    def fetch_product_breakdown(self, product_id: str) -> dict[str, dict]:
        """Все zone-rows для product_id (используем для пересчёта totals)."""
        stmt = self._stmt["select_p_breakdown"].bind((product_id,))
        stmt.consistency_level = self._read_cl
        rows = self._session.execute(stmt)
        out: dict[str, dict] = {}
        for r in rows:
            out[r.zone_id] = {
                "available_quantity": r.available_quantity or 0,
                "reserved_quantity": r.reserved_quantity or 0,
            }
        return out

    def fetch_order(self, order_id: str) -> dict | None:
        stmt = self._stmt["select_order"].bind((order_id,))
        stmt.consistency_level = self._read_cl
        row = self._session.execute(stmt).one()
        if row is None:
            return None
        return {
            "order_id": row.order_id,
            "status": row.status,
            "items": list(row.items or []),
            "last_event_ts": row.last_event_ts or 0,
        }

    # ----------------------- Batch builders ---------------------------------

    def new_batch(self) -> BatchStatement:
        b = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self._write_cl)
        return b

    def execute_batch(self, batch: BatchStatement) -> None:
        self._session.execute(batch)

    # Команды, которые добавляются в batch
    def add_upd_pz(
        self,
        batch: BatchStatement,
        product_id: str,
        zone_id: str,
        available: int,
        reserved: int,
        last_event_ts: int,
        supplier_id: str | None,
    ) -> None:
        batch.add(
            self._stmt["upd_pz"].bind(
                (available, reserved, last_event_ts, supplier_id, product_id, zone_id)
            )
        )

    def add_upd_p(
        self,
        batch: BatchStatement,
        product_id: str,
        zone_id: str,
        available: int,
        reserved: int,
        total_available: int,
        total_reserved: int,
        last_event_ts: int,
    ) -> None:
        batch.add(
            self._stmt["upd_p"].bind(
                (
                    available,
                    reserved,
                    total_available,
                    total_reserved,
                    last_event_ts,
                    product_id,
                    zone_id,
                )
            )
        )

    def add_upd_z(
        self,
        batch: BatchStatement,
        zone_id: str,
        product_id: str,
        available: int,
        reserved: int,
        last_event_ts: int,
    ) -> None:
        batch.add(
            self._stmt["upd_z"].bind(
                (available, reserved, last_event_ts, zone_id, product_id)
            )
        )

    def add_ins_processed(
        self,
        batch: BatchStatement,
        event_id: str,
        event_type: str,
        partition: int,
        offset: int,
    ) -> None:
        batch.add(
            self._stmt["ins_processed"].bind(
                (event_id, event_type, datetime.now(tz=timezone.utc), partition, offset)
            )
        )

    def add_upsert_order(
        self,
        batch: BatchStatement,
        order_id: str,
        status: str,
        items: list[tuple[str, str, int]],
        created_at: datetime,
        updated_at: datetime,
        last_event_ts: int,
    ) -> None:
        batch.add(
            self._stmt["upsert_order"].bind(
                (status, items, created_at, updated_at, last_event_ts, order_id)
            )
        )

    def add_event_log(
        self,
        batch: BatchStatement,
        bucket: str,
        event_ts: int,
        event_id: str,
        event_type: str,
        payload: str,
    ) -> None:
        batch.add(
            self._stmt["ins_event_log"].bind(
                (bucket, event_ts, event_id, event_type, payload)
            )
        )
