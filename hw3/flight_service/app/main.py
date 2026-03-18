import logging
import os
import signal
import sys
from concurrent import futures

import grpc

from app.interceptors import ApiKeyInterceptor
from app.servicer import FlightServicer

from flight.v1 import flight_pb2_grpc

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def serve():
    port = os.environ.get("GRPC_PORT", "50051")
    api_key = os.environ.get("FLIGHT_GRPC_API_KEY", "")

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[ApiKeyInterceptor(api_key)],
    )
    flight_pb2_grpc.add_FlightServiceServicer_to_server(FlightServicer(), server)
    server.add_insecure_port(f"[::]:{port}")

    def stop(*_):
        log.info("Shutting down gRPC server")
        server.stop(5)
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    server.start()
    log.info("Flight gRPC listening on :%s", port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
