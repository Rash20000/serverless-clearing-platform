"""
utils.py – Shared utilities for Job Service.

Provides:
    • Powertools Logger and Tracer singletons
    • Cached boto3 clients for DynamoDB, Lambda, and Pinpoint
    • Environment variable extraction
    • Pinpoint HTML email dispatcher with exception handling
    • Registry lookup and asynchronous Lambda invocation helpers
    • CloudFormation Custom Resource response dispatcher
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from aws_lambda_powertools import Logger, Tracer

# ──────────────────────────────────────────────
# Observability singletons
# ──────────────────────────────────────────────
SERVICE_NAME: str = os.environ.get("POWERTOOLS_SERVICE_NAME", "job-service")

logger: Logger = Logger(service=SERVICE_NAME)
tracer: Tracer = Tracer(service=SERVICE_NAME)

# ──────────────────────────────────────────────
# Module-level cached AWS clients
# ──────────────────────────────────────────────
_dynamodb_resource = None
_lambda_client = None
_pinpoint_client = None


def get_dynamodb_resource():
    """Return cached DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def get_lambda_client():
    """Return cached Lambda client."""
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


def get_pinpoint_client():
    """Return cached Pinpoint client."""
    global _pinpoint_client
    if _pinpoint_client is None:
        _pinpoint_client = boto3.client("pinpoint")
    return _pinpoint_client


# ──────────────────────────────────────────────
# Environment Helpers
# ──────────────────────────────────────────────


def get_env(name: str, default: Optional[str] = None) -> str:
    """Return an environment variable or raise EnvironmentError if missing and no default."""
    value = os.environ.get(name, default)
    if value is None:
        raise EnvironmentError(f"Required environment variable '{name}' is not set")
    return value


# ──────────────────────────────────────────────
# Notification Helpers
# ──────────────────────────────────────────────


@tracer.capture_method
def send_pinpoint_email(
    app_id: str,
    from_email: str,
    to_email: str,
    subject: str,
    html_body: str,
) -> Optional[Dict[str, Any]]:
    """
    Dispatch an HTML notification email via Amazon Pinpoint.
    Returns the Pinpoint response dict, or None on failure without raising.
    """
    if not app_id:
        logger.warning("PINPOINT_APP_ID is not configured; skipping email dispatch")
        return None

    if not from_email or not to_email:
        logger.warning(
            "Missing sender or recipient email address",
            extra={"from_email": from_email, "to_email": to_email},
        )
        return None

    pinpoint = get_pinpoint_client()
    try:
        logger.info("Sending email via Pinpoint", extra={"to": to_email, "subject": subject})
        response = pinpoint.send_messages(
            ApplicationId=app_id,
            MessageRequest={
                "Addresses": {to_email: {"ChannelType": "EMAIL"}},
                "MessageConfiguration": {
                    "EmailMessage": {
                        "FromAddress": from_email,
                        "SimpleEmail": {
                            "Subject": {"Data": subject},
                            "HtmlPart": {"Data": html_body},
                        },
                    }
                },
            },
        )
        logger.info("Pinpoint email dispatched successfully")
        return response
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to dispatch Pinpoint email alert", extra={"to": to_email, "subject": subject})
        return None


# ──────────────────────────────────────────────
# Registry & Execution Helpers
# ──────────────────────────────────────────────


@tracer.capture_method
def lookup_registered_report_job(table_name: str, job_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve registered report configuration from the Jobs catalog table.
    Schema: PK = "REPORT_REGISTRY", SK = job_id.
    """
    if not table_name or not job_id:
        return None

    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    try:
        response = table.get_item(Key={"PK": "REPORT_REGISTRY", "SK": job_id})
        item = response.get("Item")
        if item:
            logger.info("Retrieved report registration", extra={"job_id": job_id, "enabled": item.get("enabled")})
        else:
            logger.warning("Report job not found in registry", extra={"job_id": job_id, "table": table_name})
        return item
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to lookup report registry item", extra={"table": table_name, "job_id": job_id})
        return None


@tracer.capture_method
def invoke_lambda_async(function_name: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Invoke a downstream Lambda function asynchronously (InvocationType="Event").
    """
    if not function_name:
        logger.error("Cannot invoke Lambda: function_name is empty")
        return None

    lambda_client = get_lambda_client()
    try:
        logger.info("Invoking downstream Lambda asynchronously", extra={"function_name": function_name})
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        status_code = response.get("StatusCode", 200)
        logger.info("Async Lambda invocation accepted", extra={"status_code": status_code, "function": function_name})
        return response
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to invoke Lambda asynchronously", extra={"function": function_name})
        raise


# ──────────────────────────────────────────────
# CloudFormation Custom Resource Response Helper
# ──────────────────────────────────────────────


def send_cfn_response(
    event: Dict[str, Any],
    context: Any,
    response_status: str,
    response_data: Optional[Dict[str, Any]] = None,
    physical_resource_id: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    """Send CloudFormation Custom Resource lifecycle signal via presigned S3 URL."""
    response_url = event.get("ResponseURL")
    if not response_url:
        logger.warning("No ResponseURL present in event; skipping CFN signal")
        return

    log_stream = getattr(context, "log_stream_name", "unknown")
    payload = {
        "Status": response_status,
        "Reason": reason or f"See CloudWatch Log Stream: {log_stream}",
        "PhysicalResourceId": physical_resource_id or event.get("PhysicalResourceId", log_stream),
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
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("CloudFormation response sent", extra={"status_code": resp.status})
    except Exception as exc:
        logger.exception("Failed to send CloudFormation response", extra={"url": response_url})
