"""
utils.py – Shared utilities for Stream Processing Pipeline.

Provides pure functional helpers for:
- SSM Parameter Store retrieval (cached across Lambda warm starts) for recipient emails
- DynamoDB pagination & batch write utilities
- S3 client helpers
- AWS Pinpoint email dispatch (with attachment support via MIME raw email)
- Execution status recording in jobs metadata table
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from aws_lambda_powertools import Logger, Tracer

# ──────────────────────────────────────────────
#  Observability
# ──────────────────────────────────────────────
SERVICE_NAME: str = os.environ.get("POWERTOOLS_SERVICE_NAME", "stream-processing-pipeline")

logger: Logger = Logger(service=SERVICE_NAME)
tracer: Tracer = Tracer(service=SERVICE_NAME)

# ──────────────────────────────────────────────
#  Module-level cached clients
# ──────────────────────────────────────────────
_BOTO_CONFIG = Config(retries={"max_attempts": 5, "mode": "adaptive"})

_dynamodb_resource = None
_dynamodb_client = None
_ssm_client = None
_pinpoint_client = None
_s3_client = None
_ssm_cache: Dict[str, str] = {}


def get_dynamodb_resource():
    """Return a cached DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb", config=_BOTO_CONFIG)
    return _dynamodb_resource


def get_dynamodb_client():
    """Return a cached DynamoDB client."""
    global _dynamodb_client
    if _dynamodb_client is None:
        _dynamodb_client = boto3.client("dynamodb", config=_BOTO_CONFIG)
    return _dynamodb_client


def get_ssm_client():
    """Return a cached SSM client."""
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm", config=_BOTO_CONFIG)
    return _ssm_client


def get_pinpoint_client():
    """Return a cached Pinpoint client."""
    global _pinpoint_client
    if _pinpoint_client is None:
        _pinpoint_client = boto3.client("pinpoint", config=_BOTO_CONFIG)
    return _pinpoint_client


def get_s3_client():
    """Return a cached S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", config=_BOTO_CONFIG)
    return _s3_client


def get_env(name: str, default: Optional[str] = None) -> str:
    """Return an environment variable or raise EnvironmentError if missing and no default."""
    value = os.environ.get(name, default)
    if value is None:
        raise EnvironmentError(f"Required environment variable '{name}' is not set")
    return value


def get_current_date_string() -> str:
    """Return current UTC date formatted as YYYYMMDD."""
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Chunk a list into sublists of size up to chunk_size."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


# ──────────────────────────────────────────────
#  SSM Parameter Store
# ──────────────────────────────────────────────
@tracer.capture_method
def get_ssm_parameter(param_name: str, decrypt: bool = False) -> str:
    """Fetch parameter value from AWS SSM Parameter Store with in-memory caching."""
    if param_name in _ssm_cache:
        return _ssm_cache[param_name]

    client = get_ssm_client()
    logger.info("Fetching parameter from SSM", extra={"param_name": param_name})
    try:
        response = client.get_parameter(Name=param_name, WithDecryption=decrypt)
        val = response.get("Parameter", {}).get("Value", "")
        _ssm_cache[param_name] = val
        return val
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to retrieve SSM parameter", extra={"param_name": param_name})
        raise


# ──────────────────────────────────────────────
#  Job Status Tracking
# ──────────────────────────────────────────────
@tracer.capture_method
def record_job_end(
    jobs_table_name: str,
    function_name: str,
    status: str = "COMPLETED",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Insert `{function_name}-end` status into DynamoDB jobs table.
    Monitors poll or stream this record to trigger downstream jobs.

    Schema:
        PK: {function_name}-end
        SK: {date_str}#{now_iso}
    """
    if not jobs_table_name:
        logger.warning("JOBS_TABLE_NAME not set; skipping record_job_end")
        return {}

    table = get_dynamodb_resource().Table(jobs_table_name)
    now_iso = datetime.now(timezone.utc).isoformat()
    date_str = get_current_date_string()

    item = {
        "PK": f"{function_name}-end",
        "SK": f"{date_str}#{now_iso}",
        "function_name": function_name,
        "status": status,
        "completed_at": now_iso,
        "date": date_str,
        **(metadata or {}),
    }

    logger.info(
        "Recording job completion in DynamoDB",
        extra={"jobs_table": jobs_table_name, "PK": item["PK"], "SK": item["SK"], "status": status},
    )
    try:
        table.put_item(Item=item)
        return item
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to record job status in jobs table", extra={"table": jobs_table_name})
        raise


# ──────────────────────────────────────────────
#  DynamoDB Query & Batch Write
# ──────────────────────────────────────────────
@tracer.capture_method
def query_all_items(
    table: Any,
    key_condition_expression: Any,
    expression_attribute_values: Optional[Dict[str, Any]] = None,
    index_name: Optional[str] = None,
    projection_expression: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute a DynamoDB query and paginate through all pages."""
    tbl = table if not isinstance(table, str) else get_dynamodb_resource().Table(table)

    def _build_kwargs(start_key=None) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"KeyConditionExpression": key_condition_expression}
        if expression_attribute_values:
            kwargs["ExpressionAttributeValues"] = expression_attribute_values
        if index_name:
            kwargs["IndexName"] = index_name
        if projection_expression:
            kwargs["ProjectionExpression"] = projection_expression
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        return kwargs

    response = tbl.query(**_build_kwargs())
    all_items: List[Dict[str, Any]] = response.get("Items", [])
    last_key = response.get("LastEvaluatedKey")

    while last_key:
        response = tbl.query(**_build_kwargs(start_key=last_key))
        all_items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")

    return all_items


@tracer.capture_method
def batch_write_items(table_name: str, items: List[Dict[str, Any]], batch_size: int = 25) -> int:
    """Batch-write items in parallel with retry for UnprocessedItems."""
    if not items:
        return 0

    batch_size = min(batch_size, 25)
    chunks = chunk_list(items, batch_size)
    client = get_dynamodb_resource()
    max_retries = 5

    def _write_chunk(chunk: List[Dict[str, Any]]) -> None:
        req = {table_name: [{"PutRequest": {"Item": it}} for it in chunk]}
        for _ in range(max_retries):
            resp = client.meta.client.batch_write_item(RequestItems=req)
            unprocessed = resp.get("UnprocessedItems", {})
            if not unprocessed:
                return
            req = unprocessed
        if unprocessed:
            raise RuntimeError(f"Unprocessed items remained after {max_retries} retries in {table_name}")

    with ThreadPoolExecutor(max_workers=min(10, len(chunks))) as executor:
        list(executor.map(_write_chunk, chunks))

    return len(items)


# ──────────────────────────────────────────────
#  Amazon Pinpoint Email Dispatch
# ──────────────────────────────────────────────
@tracer.capture_method
def send_email_via_pinpoint(
    application_id: str,
    from_address: str,
    to_addresses: List[str],
    subject: str,
    html_body: str,
    attachment_bytes: Optional[bytes] = None,
    attachment_filename: str = "report.csv",
) -> Dict[str, Any]:
    """
    Send an email with an optional attachment to recipient addresses via Amazon Pinpoint.
    Uses SendMessages API with Raw email message format to support MIME attachments.
    """
    if not application_id or not to_addresses:
        logger.warning(
            "Missing Pinpoint application_id or to_addresses",
            extra={"application_id": application_id, "recipients": to_addresses},
        )
        return {"status": "SKIPPED", "reason": "Missing parameters"}

    client = get_pinpoint_client()

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = ", ".join(to_addresses)

    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(body_part)

    if attachment_bytes:
        attachment = MIMEApplication(attachment_bytes)
        attachment.add_header("Content-Disposition", "attachment", filename=attachment_filename)
        msg.attach(attachment)

    raw_message_data = msg.as_bytes()

    message_request = {
        "Addresses": {email: {"ChannelType": "EMAIL"} for email in to_addresses},
        "MessageConfiguration": {
            "EmailMessage": {
                "FromAddress": from_address,
                "RawEmail": {"Data": raw_message_data},
            }
        },
    }

    logger.info(
        "Sending email via Pinpoint",
        extra={
            "application_id": application_id,
            "recipient_count": len(to_addresses),
            "subject": subject,
            "attachment": attachment_filename if attachment_bytes else None,
        },
    )

    try:
        response = client.send_messages(
            ApplicationId=application_id,
            MessageRequest=message_request,
        )
        return response
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to deliver Pinpoint report email", extra={"application_id": application_id})
        raise
