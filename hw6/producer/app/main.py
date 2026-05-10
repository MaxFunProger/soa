"""Точка входа WMS-продюсера."""
from __future__ import annotations

import logging

import uvicorn

from .api import build_app
from .bootstrap import ensure_topics, register_schemas
from .config import load_settings
from .generator import GeneratorThread
from .kafka_producer import WMSProducer


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level)
    log = logging.getLogger("producer")

    ensure_topics(settings)
    schemas = register_schemas(settings)
    log.info("registered %d Avro schemas (with V1 + V2 of ProductReceived)", len(schemas))

    producer = WMSProducer(settings, schemas)

    generator: GeneratorThread | None = None
    if settings.generator_enabled:
        generator = GeneratorThread(producer, events_per_second=settings.generator_eps)
        generator.start()

    app = build_app(producer)

    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level=settings.log_level.lower())
    finally:
        log.info("shutting down producer")
        if generator is not None:
            generator.stop()
        producer.close()


if __name__ == "__main__":
    main()
