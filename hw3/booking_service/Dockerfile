FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY booking_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto ./proto
RUN PROTO_ROOT=$(python -c "import grpc_tools, os; print(os.path.join(os.path.dirname(grpc_tools.__file__), '_proto'))") && \
    mkdir -p gen && python -m grpc_tools.protoc -Iproto -I"$PROTO_ROOT" \
    --python_out=gen --grpc_python_out=gen proto/flight/v1/flight.proto && \
    touch gen/flight/__init__.py gen/flight/v1/__init__.py

ENV PYTHONPATH=/app/gen:/app

COPY booking_service/alembic.ini .
COPY booking_service/alembic ./alembic
COPY booking_service/app ./app
COPY booking_service/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/docker-entrypoint.sh"]
