"""Обработчики складских событий.

Принципы:
* Все денормализованные таблицы обновляются в одной logged BATCH (атомарно с точки
  зрения Cassandra: либо все мутации применятся, либо ни одна).
* Идемпотентность: при входе проверяется processed_events; в той же BATCH добавляется
  отметка об успешной обработке (с TTL).
* Out-of-order: событие игнорируется, если его timestamp не строго больше последнего
  обработанного для (product_id, zone_id) (или для order_id).
* Валидация: невалидные события возбуждают ValidationError, который ловится в основном
  цикле и отправляется в DLQ.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Message

from .cassandra_store import CassandraStore

log = logging.getLogger(__name__)


class ValidationError(Exception):
    """Ошибка бизнес-валидации (->DLQ)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class HandlerError(Exception):
    """Ошибка обработки, не относящаяся к валидации (->DLQ + лог)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _bucket(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date().isoformat()


def _normalize_event(record_name: str, value: dict) -> dict:
    """Avro-deserialize иногда отдаёт union'ы как ('string', 'value') или {"string": "value"}."""
    if record_name in {"ProductReceived"}:
        sup = value.get("supplier_id")
        if isinstance(sup, tuple) and len(sup) == 2:
            value["supplier_id"] = sup[1]
        elif isinstance(sup, dict) and len(sup) == 1:
            value["supplier_id"] = next(iter(sup.values()))
    if record_name in {"ProductReserved", "ProductReleased"}:
        oid = value.get("order_id")
        if isinstance(oid, tuple) and len(oid) == 2:
            value["order_id"] = oid[1]
        elif isinstance(oid, dict) and len(oid) == 1:
            value["order_id"] = next(iter(oid.values()))
    return value


def _common_validate(value: dict) -> None:
    if not value.get("event_id"):
        raise ValidationError("VALIDATION_ERROR", "missing event_id")
    if "timestamp" not in value:
        raise ValidationError("VALIDATION_ERROR", "missing timestamp")


def _check_quantity(qty: int, name: str = "quantity") -> None:
    if qty <= 0:
        raise ValidationError(
            "VALIDATION_ERROR",
            f"Invalid {name}: {qty} (must be positive)",
        )


# ============================================================================
#                                  Handlers
# ============================================================================


class EventHandlers:
    """Логика применения событий к состоянию.

    Все handlers работают по контракту: получают (record_name, value, msg, store) и
    делают:
      1. Валидируют событие (raise ValidationError при ошибке).
      2. Считывают текущее состояние (read).
      3. Проверяют идемпотентность и порядок.
      4. Готовят BATCH с обновлениями всех денормализованных таблиц + processed_events.
      5. Выполняют BATCH.
    Возвращают True если событие применено, False если skipped.
    """

    def __init__(self, store: CassandraStore) -> None:
        self.store = store

    # ---------------------- Базовый шаблон применения ------------------------

    def _apply_inventory_change(
        self,
        *,
        record_name: str,
        event_id: str,
        event_ts: int,
        msg: Message,
        # список (product_id, zone_id, delta_available, delta_reserved)
        deltas: list[tuple[str, str, int, int]],
        # set_available={(product_id, zone_id): new_value}
        set_available: dict[tuple[str, str], int] | None = None,
        supplier_set: dict[tuple[str, str], str | None] | None = None,
        order_op: dict | None = None,
        payload_for_log: dict | None = None,
    ) -> bool:
        """Универсальный апдейтер инвентаря: применяет deltas (или абсолютную установку)
        к (product, zone) и пересчитывает агрегаты.
        """
        # Идемпотентность по event_id (быстрая проверка перед чтением остального).
        if self.store.is_already_processed(event_id):
            log.info("[%s] event_id=%s already processed — skip", record_name, event_id)
            return False

        # Группируем по product_id чтобы пересчитать total один раз.
        affected_products: dict[str, set[str]] = {}
        for (pid, zid, _da, _dr) in deltas:
            affected_products.setdefault(pid, set()).add(zid)
        if set_available:
            for (pid, zid) in set_available.keys():
                affected_products.setdefault(pid, set()).add(zid)

        # Считываем текущее состояние всех затронутых (product, zone), плюс breakdown
        # для пересчёта totals.
        current_pz: dict[tuple[str, str], dict] = {}
        for pid, zones in affected_products.items():
            for zid in zones:
                current_pz[(pid, zid)] = self.store.fetch_pz(pid, zid)

        # Out-of-order: для каждой затронутой пары проверим last_event_ts.
        for (pid, zid), state in current_pz.items():
            if event_ts <= state["last_event_ts"]:
                log.info(
                    "[%s] event_id=%s ts=%d <= last_ts=%d for (%s,%s) — skip stale",
                    record_name, event_id, event_ts, state["last_event_ts"], pid, zid,
                )
                return False

        # Считаем новые значения per-zone.
        new_pz: dict[tuple[str, str], dict] = {}
        for key, state in current_pz.items():
            new_pz[key] = {
                "available_quantity": state["available_quantity"],
                "reserved_quantity": state["reserved_quantity"],
                "supplier_id": state.get("supplier_id"),
            }

        for (pid, zid, da, dr) in deltas:
            row = new_pz[(pid, zid)]
            row["available_quantity"] = row["available_quantity"] + da
            row["reserved_quantity"] = row["reserved_quantity"] + dr

        if set_available:
            for (pid, zid), new_val in set_available.items():
                new_pz[(pid, zid)]["available_quantity"] = new_val

        if supplier_set:
            for key, supplier in supplier_set.items():
                new_pz[key]["supplier_id"] = supplier

        # Жёсткие проверки бизнес-правил (нет отрицательных значений).
        for (pid, zid), row in new_pz.items():
            if row["available_quantity"] < 0:
                raise ValidationError(
                    "INSUFFICIENT_AVAILABLE",
                    f"Resulting available_quantity for ({pid},{zid}) = {row['available_quantity']} (<0)",
                )
            if row["reserved_quantity"] < 0:
                raise ValidationError(
                    "INSUFFICIENT_RESERVED",
                    f"Resulting reserved_quantity for ({pid},{zid}) = {row['reserved_quantity']} (<0)",
                )

        # Пересчёт totals по каждому затронутому product_id.
        # Для consistency обновляем ВСЕ существующие строки inventory_by_product этого product_id
        # — иначе totals «застрянут» на старых rows для не-затронутых зон.
        product_full_breakdown: dict[str, dict[str, dict]] = {}
        for pid, zones_modified in affected_products.items():
            breakdown = self.store.fetch_product_breakdown(pid)
            for zid in zones_modified:
                breakdown[zid] = {
                    "available_quantity": new_pz[(pid, zid)]["available_quantity"],
                    "reserved_quantity": new_pz[(pid, zid)]["reserved_quantity"],
                }
            product_full_breakdown[pid] = breakdown

        # Собираем BATCH.
        batch = self.store.new_batch()

        for (pid, zid), row in new_pz.items():
            self.store.add_upd_pz(
                batch, pid, zid,
                available=row["available_quantity"],
                reserved=row["reserved_quantity"],
                last_event_ts=event_ts,
                supplier_id=row.get("supplier_id"),
            )
            self.store.add_upd_z(
                batch, zid, pid,
                available=row["available_quantity"],
                reserved=row["reserved_quantity"],
                last_event_ts=event_ts,
            )

        for pid, breakdown in product_full_breakdown.items():
            total_avail = sum(b["available_quantity"] for b in breakdown.values())
            total_reserved = sum(b["reserved_quantity"] for b in breakdown.values())
            for zid, brow in breakdown.items():
                self.store.add_upd_p(
                    batch, pid, zid,
                    available=brow["available_quantity"],
                    reserved=brow["reserved_quantity"],
                    total_available=total_avail,
                    total_reserved=total_reserved,
                    last_event_ts=event_ts,
                )

        # Опционально — операция над заказом
        if order_op is not None:
            self.store.add_upsert_order(
                batch,
                order_id=order_op["order_id"],
                status=order_op["status"],
                items=order_op["items"],
                created_at=order_op["created_at"],
                updated_at=order_op["updated_at"],
                last_event_ts=event_ts,
            )

        # idempotency record
        self.store.add_ins_processed(
            batch,
            event_id=event_id,
            event_type=record_name,
            partition=msg.partition(),
            offset=msg.offset(),
        )

        # audit log
        if payload_for_log is not None:
            self.store.add_event_log(
                batch,
                bucket=_bucket(event_ts),
                event_ts=event_ts,
                event_id=event_id,
                event_type=record_name,
                payload=json.dumps(payload_for_log, ensure_ascii=False, default=str),
            )

        self.store.execute_batch(batch)
        return True

    # ============================== Handlers =================================

    def handle_product_received(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        _check_quantity(int(value["quantity"]))
        product_id = value["product_id"]
        zone_id = value["zone_id"]
        qty = int(value["quantity"])
        supplier_id = value.get("supplier_id")
        return self._apply_inventory_change(
            record_name="ProductReceived",
            event_id=value["event_id"],
            event_ts=int(value["timestamp"]),
            msg=msg,
            deltas=[(product_id, zone_id, qty, 0)],
            supplier_set=(
                {(product_id, zone_id): supplier_id} if supplier_id is not None else None
            ),
            payload_for_log=value,
        )

    def handle_product_shipped(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        _check_quantity(int(value["quantity"]))
        product_id = value["product_id"]
        zone_id = value["zone_id"]
        qty = int(value["quantity"])
        return self._apply_inventory_change(
            record_name="ProductShipped",
            event_id=value["event_id"],
            event_ts=int(value["timestamp"]),
            msg=msg,
            deltas=[(product_id, zone_id, -qty, 0)],
            payload_for_log=value,
        )

    def handle_product_moved(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        _check_quantity(int(value["quantity"]))
        if value["from_zone_id"] == value["to_zone_id"]:
            raise ValidationError(
                "VALIDATION_ERROR",
                "from_zone_id == to_zone_id (move is no-op)",
            )
        product_id = value["product_id"]
        from_zone = value["from_zone_id"]
        to_zone = value["to_zone_id"]
        qty = int(value["quantity"])
        return self._apply_inventory_change(
            record_name="ProductMoved",
            event_id=value["event_id"],
            event_ts=int(value["timestamp"]),
            msg=msg,
            deltas=[
                (product_id, from_zone, -qty, 0),
                (product_id, to_zone, qty, 0),
            ],
            payload_for_log=value,
        )

    def handle_product_reserved(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        _check_quantity(int(value["quantity"]))
        product_id = value["product_id"]
        zone_id = value["zone_id"]
        qty = int(value["quantity"])
        return self._apply_inventory_change(
            record_name="ProductReserved",
            event_id=value["event_id"],
            event_ts=int(value["timestamp"]),
            msg=msg,
            deltas=[(product_id, zone_id, -qty, qty)],
            payload_for_log=value,
        )

    def handle_product_released(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        _check_quantity(int(value["quantity"]))
        product_id = value["product_id"]
        zone_id = value["zone_id"]
        qty = int(value["quantity"])
        return self._apply_inventory_change(
            record_name="ProductReleased",
            event_id=value["event_id"],
            event_ts=int(value["timestamp"]),
            msg=msg,
            deltas=[(product_id, zone_id, qty, -qty)],
            payload_for_log=value,
        )

    def handle_inventory_counted(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        cnt = int(value["counted_quantity"])
        if cnt < 0:
            raise ValidationError(
                "VALIDATION_ERROR",
                f"Invalid counted_quantity: {cnt} (must be >=0)",
            )
        product_id = value["product_id"]
        zone_id = value["zone_id"]
        return self._apply_inventory_change(
            record_name="InventoryCounted",
            event_id=value["event_id"],
            event_ts=int(value["timestamp"]),
            msg=msg,
            deltas=[],
            set_available={(product_id, zone_id): cnt},
            payload_for_log=value,
        )

    def handle_order_created(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        order_id = value["order_id"]
        items_raw = value.get("items") or []
        if not items_raw:
            raise ValidationError("VALIDATION_ERROR", "OrderCreated: empty items list")

        # Идемпотентность для заказа отдельно: повторный OrderCreated не повторяет резервы.
        if self.store.is_already_processed(value["event_id"]):
            log.info("[OrderCreated] event_id=%s already processed — skip", value["event_id"])
            return False

        existing = self.store.fetch_order(order_id)
        event_ts = int(value["timestamp"])
        if existing is not None and existing["last_event_ts"] >= event_ts:
            log.info(
                "[OrderCreated] order=%s last_ts=%d >= ts=%d — skip stale",
                order_id, existing["last_event_ts"], event_ts,
            )
            return False

        items_tuple: list[tuple[str, str, int]] = []
        deltas: list[tuple[str, str, int, int]] = []
        for it in items_raw:
            qty = int(it["quantity"])
            _check_quantity(qty)
            items_tuple.append((it["product_id"], it["zone_id"], qty))
            deltas.append((it["product_id"], it["zone_id"], -qty, qty))

        now = datetime.now(tz=timezone.utc)

        return self._apply_inventory_change(
            record_name="OrderCreated",
            event_id=value["event_id"],
            event_ts=event_ts,
            msg=msg,
            deltas=deltas,
            order_op={
                "order_id": order_id,
                "status": "CREATED",
                "items": items_tuple,
                "created_at": now,
                "updated_at": now,
            },
            payload_for_log=value,
        )

    def handle_order_completed(self, value: dict, msg: Message) -> bool:
        _common_validate(value)
        order_id = value["order_id"]
        if self.store.is_already_processed(value["event_id"]):
            log.info("[OrderCompleted] event_id=%s already processed — skip", value["event_id"])
            return False

        existing = self.store.fetch_order(order_id)
        if existing is None:
            raise ValidationError(
                "ORDER_NOT_FOUND",
                f"OrderCompleted: order_id={order_id} not found",
            )
        if existing["status"] == "COMPLETED":
            log.info("[OrderCompleted] order=%s already COMPLETED — skip", order_id)
            # Всё равно записываем processed_events чтобы не пытались дальше
            batch = self.store.new_batch()
            self.store.add_ins_processed(
                batch, value["event_id"], "OrderCompleted",
                msg.partition(), msg.offset(),
            )
            self.store.execute_batch(batch)
            return False

        event_ts = int(value["timestamp"])
        if existing["last_event_ts"] >= event_ts:
            log.info(
                "[OrderCompleted] order=%s last_ts=%d >= ts=%d — skip stale",
                order_id, existing["last_event_ts"], event_ts,
            )
            return False

        # списываем reserved для каждой позиции
        deltas: list[tuple[str, str, int, int]] = []
        for (product_id, zone_id, qty) in existing["items"]:
            deltas.append((product_id, zone_id, 0, -int(qty)))

        now = datetime.now(tz=timezone.utc)

        return self._apply_inventory_change(
            record_name="OrderCompleted",
            event_id=value["event_id"],
            event_ts=event_ts,
            msg=msg,
            deltas=deltas,
            order_op={
                "order_id": order_id,
                "status": "COMPLETED",
                "items": existing["items"],
                "created_at": now,  # сервер всё равно использует updated_at
                "updated_at": now,
            },
            payload_for_log=value,
        )

    # ----------------------- Dispatch ---------------------------------------

    def dispatch(self, record_name: str, value: dict, msg: Message) -> bool:
        # Очищаем union-обёртки, которые иногда отдаёт fastavro
        value = _normalize_event(record_name, value)
        method_map = {
            "ProductReceived": self.handle_product_received,
            "ProductShipped": self.handle_product_shipped,
            "ProductMoved": self.handle_product_moved,
            "ProductReserved": self.handle_product_reserved,
            "ProductReleased": self.handle_product_released,
            "InventoryCounted": self.handle_inventory_counted,
            "OrderCreated": self.handle_order_created,
            "OrderCompleted": self.handle_order_completed,
        }
        if record_name not in method_map:
            raise ValidationError(
                "UNKNOWN_RECORD",
                f"Unknown record_name: {record_name}",
            )
        return method_map[record_name](value, msg)
