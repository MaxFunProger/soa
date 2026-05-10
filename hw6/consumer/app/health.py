"""HTTP-сервер для /metrics и /health."""
from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


class _Handler(BaseHTTPRequestHandler):
    health_check: Callable[[], bool] = staticmethod(lambda: True)

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        # Пишем только в DEBUG — иначе слишком шумно от prometheus scrape.
        log.debug("[http] %s - %s", self.client_address[0], fmt % args)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            data = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/health":
            ok = bool(_Handler.health_check())
            payload = b'{"status":"ok"}' if ok else b'{"status":"down"}'
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(404)
        self.end_headers()


def start_http_server(port: int, health_check: Callable[[], bool]) -> ThreadingHTTPServer:
    _Handler.health_check = staticmethod(health_check)
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="http-server")
    thread.start()
    log.info("HTTP server listening on :%d (/metrics, /health)", port)
    return server
