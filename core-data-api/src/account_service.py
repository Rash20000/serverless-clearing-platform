"""
account_service.py – Account Inquiry API handler.

Endpoint: POST /v1/account/inquiry
Uses Powertools APIGatewayProxyEvent data class for typed, zero-bloat request parsing.
Implements robust timeout and error handling with standard HTTP status codes.
Handles trading hours warm-up pings.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import BotoCoreError, ClientError

from router_service import error_response, success_response
from utils import (
    call_outbound_gateway,
    parallel_cache_queries,
    put_event,
    query_cache_table,
)

logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "core-data-api"))
tracer = Tracer(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "core-data-api"))

_CACHE_ACCOUNT_TABLE = os.environ.get("CACHE_ACCOUNT_TABLE", "cache.account")
_CACHE_ACTIVITY_TABLE = os.environ.get("CACHE_ACTIVITY_TABLE", "cache.activity")


@event_source(data_class=APIGatewayProxyEvent)
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """POST /v1/account/inquiry handler (Critical API backed by Provisioned Concurrency)."""
    raw_event = event.raw_event
    if raw_event.get("httpMethod") == "OPTIONS":
        return success_response({"status": "ok"})
    if raw_event.get("source") == "aws.events" or raw_event.get("action") == "warmup":
        logger.info("Warmup ping received — container kept warm")
        return {"statusCode": 200, "body": json.dumps({"status": "warm"})}

    try:
        body = event.json_body
        if not body or not isinstance(body, dict):
            return error_response("Invalid or missing JSON body", status_code=400)

        account_id = body.get("account_id")
        if not account_id:
            return error_response("account_id is required", status_code=400)

        fetch_activity = body.get("fetch_activity", False)

        # 1. Cache lookup (parallel if activity requested)
        try:
            if fetch_activity:
                queries = [
                    {"table_name": _CACHE_ACCOUNT_TABLE, "partition_key": "accountId", "partition_value": account_id},
                    {"table_name": _CACHE_ACTIVITY_TABLE, "partition_key": "accountId", "partition_value": account_id},
                ]
                results = parallel_cache_queries(queries, max_workers=2)
                account_items, activity_items = results[0], results[1]
            else:
                account_items = query_cache_table(
                    table_name=_CACHE_ACCOUNT_TABLE,
                    partition_key="accountId",
                    partition_value=account_id,
                )
                activity_items = []
        except (ClientError, BotoCoreError) as db_err:
            logger.exception("Database error while reading cache tables", extra={"account_id": account_id})
            return error_response("Upstream cache query failure", status_code=502)

        # 2. Outbound gateway fallback (on cache miss)
        outbound_data: Dict[str, Any] = {}
        if not account_items:
            logger.info("Cache miss – calling outbound gateway", extra={"account_id": account_id})
            try:
                outbound_resp = call_outbound_gateway(
                    path="/v1/account/lookup",
                    payload={"account_id": account_id},
                )
                if outbound_resp.get("status") == 200:
                    outbound_data = json.loads(outbound_resp.get("body", "{}"))
                else:
                    logger.warning("Outbound gateway returned non-200", extra={"resp": outbound_resp})
            except Exception as egress_err:
                logger.exception("Outbound gateway call failed or timed out", extra={"account_id": account_id})
                # Non-blocking graceful degradation — continue with empty profile
                outbound_data = {"error": "Outbound enrichment unavailable"}

        # 3. Fire-and-forget audit event
        try:
            put_event(
                source="inbound.api",
                detail_type="QuoteCountIncremented",
                detail={"accountId": account_id},
            )
        except Exception:
            logger.warning("Failed to emit audit event to EventBridge", extra={"account_id": account_id})

        return success_response({
            "accountId": account_id,
            "cached": bool(account_items),
            "account": account_items[0] if account_items else outbound_data,
            "activity": activity_items if fetch_activity else None,
        })

    except Exception as exc:
        logger.exception("Unhandled error processing account inquiry")
        return error_response("Internal server error", status_code=500)
