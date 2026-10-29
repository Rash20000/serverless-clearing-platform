"""
utils.py – Shared helpers for the Inbound API Gateway microservices.

Provides:
    • EventBridge put-event helpers
    • HTTP connection-pooled client for outbound calls
    • DynamoDB cache-table query wrappers

All exports are pure functions.  Module-level singletons (boto3 clients,
urllib3 pool) are created once per Lambda cold-start and reused across
invocations for connection efficiency.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence

import boto3
import urllib3
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.conditions import Key

# ──────────────────────────────────────────────
# Powertools singletons
# ──────────────────────────────────────────────
logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "inbound-api"))
tracer = Tracer(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "inbound-api"))

# ──────────────────────────────────────────────
# AWS clients (cold-start once, reuse per invoke)
# ──────────────────────────────────────────────
_eventbridge_client = boto3.client("events")
_dynamodb_resource = boto3.resource("dynamodb")

# ──────────────────────────────────────────────
# HTTP connection pool (reused across invocations)
# ──────────────────────────────────────────────
_http_pool = urllib3.PoolManager(
    maxsize=10,
    retries=urllib3.Retry(total=3, backoff_factor=0.3),
    timeout=urllib3.Timeout(connect=5.0, read=10.0),
)

# ──────────────────────────────────────────────
# Environment helpers
# ──────────────────────────────────────────────
_ENV_EVENT_BUS_NAME = "EVENT_BUS_NAME"
_ENV_OUTBOUND_API_URL = "OUTBOUND_API_GATEWAY_URL"
_MAX_EB_ENTRIES = 10  # EventBridge PutEvents batch limit


def _require_env(key: str) -> str:
    """Return an environment variable or raise with a clear message."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EventBridge Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@tracer.capture_method
def put_event(
    source: str,
    detail_type: str,
    detail: Dict[str, Any],
    event_bus_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Put a single event onto the EventBridge bus.

    Parameters
    ----------
    source : str
        Event source identifier (e.g. ``"inbound.api"``).
    detail_type : str
        Event detail-type (e.g. ``"QuoteCountIncremented"``).
    detail : dict
        Arbitrary JSON-serialisable event payload.
    event_bus_name : str, optional
        Override bus name; defaults to ``EVENT_BUS_NAME`` env var.

    Returns
    -------
    dict
        Raw ``PutEvents`` response from EventBridge.
    """
    bus = event_bus_name or _require_env(_ENV_EVENT_BUS_NAME)
    entry = {
        "Source": source,
        "DetailType": detail_type,
        "Detail": json.dumps(detail, default=str),
        "EventBusName": bus,
    }
    logger.info("Putting EventBridge event", extra={"entry": entry})
    response = _eventbridge_client.put_events(Entries=[entry])
    if response.get("FailedEntryCount", 0) > 0:
        logger.error("EventBridge put_events partial failure", extra={"response": response})
    return response


@tracer.capture_method
def put_events_batch(
    entries: Sequence[Dict[str, Any]],
    event_bus_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Put multiple events in batches of 10 (EventBridge limit).

    Each element in *entries* must contain ``source``, ``detail_type``,
    and ``detail`` keys.

    Returns a list of raw PutEvents responses.
    """
    bus = event_bus_name or _require_env(_ENV_EVENT_BUS_NAME)
    formatted: List[Dict[str, Any]] = [
        {
            "Source": e["source"],
            "DetailType": e["detail_type"],
            "Detail": json.dumps(e["detail"], default=str),
            "EventBusName": bus,
        }
        for e in entries
    ]

    responses: List[Dict[str, Any]] = []
    for start in range(0, len(formatted), _MAX_EB_ENTRIES):
        batch = formatted[start : start + _MAX_EB_ENTRIES]
        resp = _eventbridge_client.put_events(Entries=batch)
        responses.append(resp)
        if resp.get("FailedEntryCount", 0) > 0:
            logger.warning(
                "EventBridge batch partial failure",
                extra={"batch_start": start, "response": resp},
            )
    return responses


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTTP Client (Connection-Pooled)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@tracer.capture_method
def http_request(
    method: str,
    url: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Issue an HTTP request using the module-level connection pool.

    Parameters
    ----------
    method : str
        HTTP verb (``GET``, ``POST``, etc.).
    url : str
        Fully qualified URL.
    body : dict, optional
        JSON-serialisable request body.
    headers : dict, optional
        Additional HTTP headers.
    timeout : float, optional
        Per-request timeout override in seconds.

    Returns
    -------
    dict
        ``{"status": int, "body": str, "headers": dict}``
    """
    req_headers = {"Content-Type": "application/json", **(headers or {})}
    encoded_body = json.dumps(body, default=str).encode("utf-8") if body else None

    logger.debug("HTTP request", extra={"method": method, "url": url})

    resp = _http_pool.request(
        method,
        url,
        body=encoded_body,
        headers=req_headers,
        timeout=timeout or 10.0,
    )

    return {
        "status": resp.status,
        "body": resp.data.decode("utf-8"),
        "headers": dict(resp.headers),
    }


@tracer.capture_method
def call_outbound_gateway(
    path: str,
    payload: Dict[str, Any],
    *,
    method: str = "POST",
) -> Dict[str, Any]:
    """Call the Outbound API Gateway via its imported URL.

    The base URL is read from ``OUTBOUND_API_GATEWAY_URL`` env var, which
    is populated from ``Fn::ImportValue: OutboundApiGatewayExecuteEndpointUrl``
    at deploy time.
    """
    base_url = _require_env(_ENV_OUTBOUND_API_URL).rstrip("/")
    full_url = f"{base_url}/{path.lstrip('/')}"
    logger.info("Calling outbound gateway", extra={"url": full_url})
    return http_request(method, full_url, body=payload)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DynamoDB Cache Query Wrappers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@tracer.capture_method
def query_cache_table(
    table_name: str,
    partition_key: str,
    partition_value: str,
    *,
    sort_key: Optional[str] = None,
    sort_value: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Query a DynamoDB cache table by partition (and optional sort) key.

    Parameters
    ----------
    table_name : str
        DynamoDB table name (e.g. ``cache.account``).
    partition_key / partition_value : str
        Partition key name and value.
    sort_key / sort_value : str, optional
        Sort key name and value for an exact match.
    limit : int
        Maximum items to return.

    Returns
    -------
    list[dict]
        List of DynamoDB items (deserialised).
    """
    table = _dynamodb_resource.Table(table_name)
    key_expr = Key(partition_key).eq(partition_value)
    if sort_key and sort_value:
        key_expr = key_expr & Key(sort_key).eq(sort_value)

    logger.info(
        "Querying cache table",
        extra={"table": table_name, "pk": partition_value},
    )
    response = table.query(KeyConditionExpression=key_expr, Limit=limit)
    return response.get("Items", [])


@tracer.capture_method
def get_cache_item(
    table_name: str,
    key: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    """Get a single item from a DynamoDB cache table.

    Parameters
    ----------
    table_name : str
        DynamoDB table name.
    key : dict
        Primary key dict (e.g. ``{"accountId": "ACC123"}``).

    Returns
    -------
    dict or None
        The item if found, else ``None``.
    """
    table = _dynamodb_resource.Table(table_name)
    response = table.get_item(Key=key)
    return response.get("Item")


@tracer.capture_method
def parallel_cache_queries(
    queries: List[Dict[str, Any]],
    *,
    max_workers: int = 5,
) -> List[List[Dict[str, Any]]]:
    """Run multiple cache-table queries in parallel.

    Each element in *queries* is a kwargs dict passed to
    :func:`query_cache_table`.

    Parameters
    ----------
    queries : list[dict]
        List of keyword-argument dicts for ``query_cache_table``.
    max_workers : int
        Thread pool size.

    Returns
    -------
    list[list[dict]]
        Results in the same order as the input queries.
    """
    results: List[Optional[List[Dict[str, Any]]]] = [None] * len(queries)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_idx = {
            pool.submit(query_cache_table, **q): idx
            for idx, q in enumerate(queries)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception:
                logger.exception(
                    "Parallel cache query failed",
                    extra={"query_index": idx, "query": queries[idx]},
                )
                results[idx] = []

    return results  # type: ignore[return-value]
