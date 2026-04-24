"""Entry point for the producer service."""
from __future__ import annotations

import logging
from pathlib import Path

import uvicorn

from .api import build_app
from .bootstrap import ensure_topic, register_schema
from .config import load_settings
from .generator import GeneratorThread
from .kafka_producer import MovieEventProducer


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level)
    log = logging.getLogger("producer")

    schema_str = Path(settings.schema_path).read_text(encoding="utf-8")

    ensure_topic(settings)
    register_schema(settings, schema_str)

    producer = MovieEventProducer(settings, schema_str)

    generator: GeneratorThread | None = None
    if settings.generator_enabled:
        generator = GeneratorThread(producer, events_per_second=settings.generator_eps)
        generator.start()

    app = build_app(producer)

    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level=settings.log_level.lower())
    finally:
        log.info("Shutting down producer")
        if generator is not None:
            generator.stop()
        producer.close()


if __name__ == "__main__":
    main()
