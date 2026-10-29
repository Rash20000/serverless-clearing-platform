"""
pages_service.py – Pages Lookup API handler.

Endpoint: GET /v1/pages/{account_id}
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
from utils import query_cache_table

logger = Logger(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "core-data-api"))
tracer = Tracer(service=os.environ.get("POWERTOOLS_SERVICE_NAME", "core-data-api"))

_CACHE_PAGES_TABLE = os.environ.get("CACHE_PAGES_TABLE", "cache.pages")


@event_source(data_class=APIGatewayProxyEvent)
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    """GET /v1/pages/{account_id} handler."""
    raw_event = event.raw_event
    if raw_event.get("httpMethod") == "OPTIONS":
        return success_response({"status": "ok"})
    if raw_event.get("source") == "aws.events" or raw_event.get("action") == "warmup":
        logger.info("Warmup ping received — container kept warm")
        return {"statusCode": 200, "body": json.dumps({"status": "warm"})}

    try:
        path_params = event.path_parameters or {}
        account_id = path_params.get("account_id", "")

        if not account_id:
            return error_response("account_id path parameter is required", status_code=400)

        try:
            items = query_cache_table(
                table_name=_CACHE_PAGES_TABLE,
                partition_key="accountId",
                partition_value=account_id,
            )
        except (ClientError, BotoCoreError) as db_err:
            logger.exception("Failed to query pages cache table", extra={"account_id": account_id})
            return error_response("Upstream database query failure", status_code=502)

        return success_response({"accountId": account_id, "pages": items})

    except Exception as exc:
        logger.exception("Unhandled error processing pages lookup")
        return error_response("Internal server error", status_code=500)
