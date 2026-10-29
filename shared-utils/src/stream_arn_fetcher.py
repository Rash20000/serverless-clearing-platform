"""
stream_arn_fetcher.py – CloudFormation Custom Resource: StreamArnFetcher using crhelper.

Dynamically fetches the LatestStreamArn for a DynamoDB table during CloudFormation stack
creation/updates. When DynamoDB tables are redeployed or recreated, their Stream ARNs change.
This Custom Resource inspects the active table via describe_table and returns the live StreamArn,
preventing CloudFormation EventSourceMapping breakage across stack recreations.
"""

from __future__ import annotations

from typing import Any, Dict

from crhelper import CfnResource
from botocore.exceptions import BotoCoreError, ClientError

from utils import (
    get_dynamodb_client,
    logger,
    tracer,
)

helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")


def fetch_latest_stream_arn(table_name: str) -> str:
    """
    Call DynamoDB DescribeTable to resolve the dynamic LatestStreamArn.
    Raises ValueError if table does not have an active stream.
    """
    ddb = get_dynamodb_client()
    logger.info("Describing DynamoDB table for stream discovery", extra={"table_name": table_name})
    response = ddb.describe_table(TableName=table_name)
    stream_arn = response.get("Table", {}).get("LatestStreamArn")

    if not stream_arn:
        raise ValueError(f"Table '{table_name}' does not have an active DynamoDB Stream enabled.")

    return stream_arn


@helper.create
@helper.update
def create_or_update(event: Dict[str, Any], context: Any) -> str:
    """Fetch LatestStreamArn and populate helper.Data['StreamArn']."""
    props = event.get("ResourceProperties", {})
    table_name = props.get("TableName", "")
    physical_id = f"{table_name or 'unknown'}-stream-arn"

    if not table_name:
        raise ValueError("TableName is required in ResourceProperties for StreamArnFetcher")

    stream_arn = fetch_latest_stream_arn(table_name)
    logger.info("Discovered active Stream ARN", extra={"table_name": table_name, "stream_arn": stream_arn})
    helper.Data["StreamArn"] = stream_arn
    return physical_id


@helper.delete
def delete(event: Dict[str, Any], context: Any) -> str:
    """No-op on delete; return physical ID."""
    props = event.get("ResourceProperties", {})
    table_name = props.get("TableName", "")
    return f"{table_name or 'unknown'}-stream-arn"


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """Custom resource handler for StreamArnFetcher via crhelper."""
    helper(event, context)


# Backwards compatibility alias
handler = lambda_handler
