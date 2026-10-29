"""
router_service.py – Pure-functional route resolver for the Inbound API Gateway.

Maps API Gateway resource paths to handler functions without any class-based
routing.  Each route is a simple dict entry; resolution is a pure lookup.
"""

from __future__ import annotations

import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from aws_lambda_powertools import Logger

logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "inbound-api"))

# ──────────────────────────────────────────────
# Type aliases
# ──────────────────────────────────────────────
HandlerFn = Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]
Route = Dict[str, Any]  # {"method": str, "pattern": re.Pattern, "handler": HandlerFn, "name": str}

# ──────────────────────────────────────────────
# Route table builder
# ──────────────────────────────────────────────

def _compile_path(path_template: str) -> re.Pattern:
    """Convert a path template like ``/v1/account/{id}`` into a regex."""
    pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", path_template)
    return re.compile(f"^{pattern}$")


def build_route_table(
    routes: List[Tuple[str, str, HandlerFn, str]],
) -> List[Route]:
    """Build an ordered route table from ``(method, path, handler, name)`` tuples.

    Parameters
    ----------
    routes : list[tuple]
        Each element is ``(HTTP_METHOD, path_template, handler_fn, route_name)``.

    Returns
    -------
    list[Route]
        Compiled route entries ready for :func:`resolve_route`.

    Example
    -------
    >>> table = build_route_table([
    ...     ("POST", "/v1/account/inquiry", handle_account_inquiry, "account-inquiry"),
    ...     ("GET",  "/v1/activity/{account_id}", handle_activity, "activity-lookup"),
    ... ])
    """
    return [
        {
            "method": method.upper(),
            "pattern": _compile_path(path),
            "handler": handler,
            "name": name,
        }
        for method, path, handler, name in routes
    ]


# ──────────────────────────────────────────────
# Route resolution (pure function)
# ──────────────────────────────────────────────

def resolve_route(
    route_table: List[Route],
    http_method: str,
    path: str,
) -> Optional[Tuple[HandlerFn, Dict[str, str], str]]:
    """Resolve an HTTP method + path against the route table.

    Parameters
    ----------
    route_table : list[Route]
        Output of :func:`build_route_table`.
    http_method : str
        ``GET``, ``POST``, etc.
    path : str
        Request path (e.g. ``/v1/account/inquiry``).

    Returns
    -------
    tuple or None
        ``(handler_fn, path_params, route_name)`` if matched, else ``None``.
    """
    method = http_method.upper()
    for route in route_table:
        if route["method"] != method:
            continue
        match = route["pattern"].match(path)
        if match:
            logger.info(
                "Route resolved",
                extra={"route": route["name"], "path": path},
            )
            return route["handler"], match.groupdict(), route["name"]

    logger.warning("No route matched", extra={"method": method, "path": path})
    return None


# ──────────────────────────────────────────────
# Response builders (pure helpers)
# ──────────────────────────────────────────────

def success_response(body: Any, *, status_code: int = 200) -> Dict[str, Any]:
    """Build a successful API Gateway proxy response with security and CORS headers."""
    import json

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Content-Type-Options": "nosniff",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }


def error_response(
    message: str,
    *,
    status_code: int = 400,
    error_code: Optional[str] = None,
) -> Dict[str, Any]:
    """Build an error API Gateway proxy response with security and CORS headers."""
    import json

    payload: Dict[str, Any] = {"message": message}
    if error_code:
        payload["errorCode"] = error_code
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Content-Type-Options": "nosniff",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(payload),
    }
