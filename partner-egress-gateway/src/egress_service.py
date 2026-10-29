"""
egress_service.py – Outbound Financial Market Data Egress Gateway.

Uses AWS Lambda Powertools APIGatewayRestResolver to route internal requests
to external financial data providers and exchanges:
  • Primary Exchange              — Equity quotes
  • Secondary Exchange            — Equity quotes
  • Reference Vendor              — Security master reference data
  • Batch Market Data Dispatch    — Concurrent multi-symbol queries
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, Response
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    InternalServerError,
    NotFoundError,
)
from aws_lambda_powertools.metrics import MetricUnit

from utils import (
    evict_vendor_secret,
    get_vendor_base_url,
    get_vendor_secret,
    send_vendor_http_request,
)

logger = Logger(service="partner-egress-gateway")
tracer = Tracer(service="partner-egress-gateway")
metrics = Metrics(namespace="OutboundGateway", service="partner-egress-gateway")

app = APIGatewayRestResolver()

# Default URLs from environment variables, with fallback defaults
DEFAULT_PRIMARY_EXCHANGE_URL = os.environ.get("PRIMARY_EXCHANGE_BASE_URL", "https://api.exchange-primary.example.com/api")
DEFAULT_SECONDARY_EXCHANGE_URL = os.environ.get("SECONDARY_EXCHANGE_BASE_URL", "https://api.exchange-secondary.example.com/api")
DEFAULT_REF_VENDOR_URL = os.environ.get("REF_VENDOR_BASE_URL", "https://api.reference-vendor.example.com/v1")


@tracer.capture_method
def call_vendor_with_retry(
    vendor: str,
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Dispatch request to external vendor. If 401 Unauthorized is returned,
    automatically evicts the cached secret, fetches the fresh secret from
    Secrets Manager, and retries the request once.
    """
    api_key = get_vendor_secret(vendor)
    req_headers = dict(headers or {})
    if vendor in ("reference-vendor", "ref-vendor"):
        req_headers["Authorization"] = f"Bearer {api_key}"
    else:
        req_headers["X-Api-Key"] = api_key

    resp = send_vendor_http_request(url=url, method=method, headers=req_headers)

    # Credential rotation self-healing: retry on 401/403 auth challenge
    if resp.get("status_code") in (401, 403):
        logger.warning(
            "Vendor returned auth failure (401/403); evicting cached secret and retrying with fresh secret",
            extra={"vendor": vendor, "status_code": resp.get("status_code")},
        )
        fresh_key = get_vendor_secret(vendor, force_refresh=True)
        if vendor in ("reference-vendor", "ref-vendor"):
            req_headers["Authorization"] = f"Bearer {fresh_key}"
        else:
            req_headers["X-Api-Key"] = fresh_key
        resp = send_vendor_http_request(url=url, method=method, headers=req_headers)

    return resp


# ──────────────────────────────────────────────
#  Routes: Individual Stock Quotes & Securities
# ──────────────────────────────────────────────

@app.get("/market-data/exchange-primary/quote/<symbol>")
@tracer.capture_method
def get_primary_exchange_quote(symbol: str) -> Dict[str, Any]:
    """Fetch equity quote from Primary Exchange."""
    clean_symbol = symbol.strip().upper()
    if not clean_symbol:
        raise BadRequestError("Symbol must not be empty")

    base_url = get_vendor_base_url("exchange-primary", DEFAULT_PRIMARY_EXCHANGE_URL)
    url = f"{base_url}/quote/{clean_symbol}"

    metrics.add_metric(name="PrimaryExchangeQuoteRequests", unit=MetricUnit.Count, value=1)
    logger.info("Fetching Primary Exchange quote", extra={"symbol": clean_symbol, "url": url})

    resp = call_vendor_with_retry(vendor="exchange-primary", url=url, method="GET")
    return {
        "exchange": "PRIMARY",
        "symbol": clean_symbol,
        "statusCode": resp["status_code"],
        "data": resp["data"],
    }


@app.get("/market-data/exchange-secondary/quote/<symbol>")
@tracer.capture_method
def get_secondary_exchange_quote(symbol: str) -> Dict[str, Any]:
    """Fetch equity quote from Secondary Exchange."""
    clean_symbol = symbol.strip().upper()
    if not clean_symbol:
        raise BadRequestError("Symbol must not be empty")

    base_url = get_vendor_base_url("exchange-secondary", DEFAULT_SECONDARY_EXCHANGE_URL)
    url = f"{base_url}/quote/{clean_symbol}"

    metrics.add_metric(name="SecondaryExchangeQuoteRequests", unit=MetricUnit.Count, value=1)
    logger.info("Fetching Secondary Exchange quote", extra={"symbol": clean_symbol, "url": url})

    resp = call_vendor_with_retry(vendor="exchange-secondary", url=url, method="GET")
    return {
        "exchange": "SECONDARY",
        "symbol": clean_symbol,
        "statusCode": resp["status_code"],
        "data": resp["data"],
    }


@app.get("/market-data/reference-vendor/security/<identifier>")
@tracer.capture_method
def get_reference_vendor_security(identifier: str) -> Dict[str, Any]:
    """Fetch security reference data from Reference Vendor."""
    clean_id = identifier.strip().upper()
    if not clean_id:
        raise BadRequestError("Identifier must not be empty")

    base_url = get_vendor_base_url("reference-vendor", DEFAULT_REF_VENDOR_URL)
    url = f"{base_url}/securities/{clean_id}"

    metrics.add_metric(name="ReferenceVendorRequests", unit=MetricUnit.Count, value=1)
    logger.info("Fetching reference vendor security master", extra={"identifier": clean_id, "url": url})

    resp = call_vendor_with_retry(vendor="reference-vendor", url=url, method="GET")
    return {
        "vendor": "REFERENCE_VENDOR",
        "identifier": clean_id,
        "statusCode": resp["status_code"],
        "data": resp["data"],
    }


# ──────────────────────────────────────────────
#  Routes: Batch Multi-Symbol Dispatch
# ──────────────────────────────────────────────

@app.post("/market-data/batch")
@tracer.capture_method
def batch_quotes() -> Dict[str, Any]:
    """
    Fetch quotes for multiple symbols concurrently.
    Payload: {"exchange": "PRIMARY"|"SECONDARY", "symbols": ["SYM1", "SYM2", "SYM3"]}
    """
    body: Dict[str, Any] = app.current_event.json_body
    exchange = (body.get("exchange") or "PRIMARY").strip().upper()
    symbols: List[str] = body.get("symbols", [])

    if not symbols:
        raise BadRequestError("Missing or empty 'symbols' list in request payload")

    if exchange not in ("PRIMARY", "SECONDARY"):
        raise BadRequestError(f"Unsupported exchange '{exchange}'. Supported: PRIMARY, SECONDARY")

    def _fetch_single(sym: str) -> Dict[str, Any]:
        try:
            if exchange == "PRIMARY":
                return get_primary_exchange_quote(sym)
            else:
                return get_secondary_exchange_quote(sym)
        except Exception as exc:
            logger.warning("Batch quote fetch failed for symbol", extra={"symbol": sym, "error": str(exc)})
            return {"symbol": sym, "error": str(exc), "status": "FAILED"}

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(symbols), 5)) as executor:
        futures = [executor.submit(_fetch_single, s) for s in symbols]
        for f in as_completed(futures):
            results.append(f.result())

    metrics.add_metric(name="BatchQuotesDispatched", unit=MetricUnit.Count, value=len(symbols))
    return {
        "exchange": exchange,
        "total_requested": len(symbols),
        "results": results,
    }


# ──────────────────────────────────────────────
#  Lambda Entry Point
# ──────────────────────────────────────────────

@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: Any) -> dict:
    """
    Entry-point for the Outbound Partner Egress Gateway.
    Handles scheduled warmup pings cleanly, and resolves REST requests via Powertools.
    """
    if event.get("action") == "warmup" or event.get("source") == "aws.events":
        logger.info("Warmup event intercepted – keeping Lambda execution environment warm")
        return {"statusCode": 200, "body": json.dumps({"status": "warm"})}

    return app.resolve(event, context)


# Backwards compatibility alias
handler = lambda_handler
