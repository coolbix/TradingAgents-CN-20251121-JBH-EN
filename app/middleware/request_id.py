"""Request ID/Trace-ID middle
- Generate a single ID (trace id) for each request, write to request.state and respond Head
- Write track id to logging constextvars, so all logs are automatically taken out
"""

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import uuid
import time
import logging
from typing import Callable

from app.core.logging_context import trace_id_var

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Request for ID and log middle (trace id)"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        #Generate request ID/trace id
        trace_id = str(uuid.uuid4())
        request.state.request_id = trace_id  #Compatible existing field names
        request.state.trace_id = trace_id

        #Write track id to contactvars
        token = trace_id_var.set(trace_id)

        #Record request start time
        start_time = time.time()

        #Record request information
        logger.info(
            f"- Trace id:{trace_id}, "
            f"Methodology:{request.method}, Path:{request.url.path}, "
            f"Client:{request.client.host if request.client else 'unknown'}"
        )

        try:
            #Processing of requests
            response = await call_next(request)

            #Calculate processing time
            process_time = time.time() - start_time

            #Add Response Header
            response.headers["X-Trace-ID"] = trace_id
            response.headers["X-Request-ID"] = trace_id  #Compatibility
            response.headers["X-Process-Time"] = f"{process_time:.3f}"

            #Record requested completion information
            logger.info(
                f"- Trace id:{trace_id}, status code:{response.status_code}, processing time:{process_time:.3f}s"
            )

            return response

        except Exception as exc:
            #Calculate processing time
            process_time = time.time() - start_time

            #Record requested abnormal information
            logger.error(
                f"Request abnormal - trace id:{trace_id}, processing time:{process_time:.3f}s, anomaly:{str(exc)}"
            )
            raise

        finally:
            #Clean up contacttvar, avoid leaking to subsequent requests
            try:
                trace_id_var.reset(token)
            except Exception:
                pass
