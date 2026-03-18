import logging
import os
from datetime import date
from typing import Any, Optional
from uuid import UUID

from redis.sentinel import Sentinel

log = logging.getLogger(__name__)

TTL = int(os.environ.get("REDIS_CACHE_TTL_SEC", "300"))
MASTER = os.environ.get("REDIS_MASTER_NAME", "mymaster")


def _parse_sentinels() -> list[tuple[str, int]]:
    raw = os.environ.get("REDIS_SENTINEL_HOSTS", "localhost:26379")
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        host, _, port = part.partition(":")
        out.append((host, int(port or 26379)))
    return out or [("localhost", 26379)]


_sentinel: Optional[Sentinel] = None
_redis = None


def get_redis():
    global _sentinel, _redis
    if _redis is not None:
        return _redis
    import time

    hosts = _parse_sentinels()
    last = None
    for attempt in range(30):
        try:
            _sentinel = Sentinel(hosts, socket_timeout=3, decode_responses=True)
            _redis = _sentinel.master_for(MASTER, socket_timeout=3, decode_responses=True)
            _redis.ping()
            log.info("Redis Sentinel connected, master=%s", MASTER)
            return _redis
        except Exception as e:
            last = e
            log.warning("Redis connect attempt %s/30: %s", attempt + 1, e)
            time.sleep(1)
    raise RuntimeError(f"Redis Sentinel unavailable: {last}")


def flight_key(flight_id: str) -> str:
    return f"flight:{flight_id}"


def search_key(origin: str, dest: str, dep: Optional[date]) -> str:
    d = dep.isoformat() if dep else "all"
    return f"search:{origin.upper()}:{dest.upper()}:{d}"


def cache_get_json(r, key: str) -> tuple[bool, Optional[Any]]:
    import json

    v = r.get(key)
    if v is None:
        log.info("cache miss key=%s", key)
        return False, None
    log.info("cache hit key=%s", key)
    return True, json.loads(v)


def cache_set_json(r, key: str, obj: Any) -> None:
    import json

    r.setex(key, TTL, json.dumps(obj, default=str))


def invalidate_flight_and_searches(r, flight_id: str, origin: str, dest: str) -> None:
    r.delete(flight_key(flight_id))
    pattern = f"search:{origin.upper()}:{dest.upper()}:*"
    for key in r.scan_iter(match=pattern, count=100):
        r.delete(key)
    log.info("cache invalidated flight=%s search_pattern=%s", flight_id, pattern)
