import logging
import os

import grpc

log = logging.getLogger(__name__)


class ApiKeyInterceptor(grpc.ServerInterceptor):
    """Проверка x-api-key в metadata для всех RPC."""

    def __init__(self, api_key: str):
        self._expected = (api_key or "").strip()

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        def wrap_unary(behavior):
            def inner(request, context):
                md = dict(context.invocation_metadata() or {})
                md_lower = {k.lower(): v for k, v in md.items()}
                got = (md_lower.get("x-api-key") or md_lower.get("authorization", "").replace("Bearer ", "").strip())
                if not self._expected or got != self._expected:
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid API key")
                return behavior(request, context)

            return inner

        if handler.request_streaming or handler.response_streaming:
            return handler

        return grpc.unary_unary_rpc_method_handler(
            wrap_unary(handler.unary_unary),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
