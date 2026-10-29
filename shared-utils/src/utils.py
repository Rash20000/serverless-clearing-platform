"""
utils.py – Shared helpers for CloudFormation Custom Resources in shared-utils.

Provides:
    • Powertools Logger and Tracer singletons
    • Cached boto3 clients and resources (S3, DynamoDB)
    • Resilient CloudFormation Custom Resource response dispatcher
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any, Dict, Optional

import boto3
from aws_lambda_powertools import Logger, Tracer

# ──────────────────────────────────────────────
#  Observability singletons
# ──────────────────────────────────────────────
SERVICE_NAME: str = os.environ.get("POWERTOOLS_SERVICE_NAME", "shared-utils")

logger: Logger = Logger(service=SERVICE_NAME)
tracer: Tracer = Tracer(service=SERVICE_NAME)

# ──────────────────────────────────────────────
#  Module-level cached AWS clients
# ──────────────────────────────────────────────
_s3_client = None
_s3_resource = None
_dynamodb_client = None


def get_s3_client():
    """Return cached S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def get_s3_resource():
    """Return cached S3 resource."""
    global _s3_resource
    if _s3_resource is None:
        _s3_resource = boto3.resource("s3")
    return _s3_resource


def get_dynamodb_client():
    """Return cached DynamoDB client."""
    global _dynamodb_client
    if _dynamodb_client is None:
        _dynamodb_client = boto3.client("dynamodb")
    return _dynamodb_client


# ──────────────────────────────────────────────
#  CloudFormation Custom Resource Response Dispatcher
# ──────────────────────────────────────────────
def send_cfn_response(
    event: Dict[str, Any],
    context: Any,
    response_status: str,
    response_data: Optional[Dict[str, Any]] = None,
    physical_resource_id: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    """
    Send lifecycle signal to CloudFormation via the presigned S3 ResponseURL.
    """
    response_url = event.get("ResponseURL")
    if not response_url:
        logger.warning("No ResponseURL found in event payload; skipping CloudFormation response")
        return

    log_stream = getattr(context, "log_stream_name", "unknown")
    fallback_id = event.get("PhysicalResourceId", log_stream)
    payload = {
        "Status": response_status,
        "Reason": reason or f"See CloudWatch Log Stream: {log_stream}",
        "PhysicalResourceId": physical_resource_id or fallback_id,
        "StackId": event.get("StackId", ""),
        "RequestId": event.get("RequestId", ""),
        "LogicalResourceId": event.get("LogicalResourceId", ""),
        "Data": response_data or {},
    }
    encoded_body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        response_url,
        data=encoded_body,
        headers={"content-type": "", "content-length": str(len(encoded_body))},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            logger.info("CloudFormation Custom Resource response delivered", extra={"status_code": resp.status})
    except Exception as exc:
        logger.exception("Failed to deliver CloudFormation Custom Resource response", extra={"error": str(exc)})
