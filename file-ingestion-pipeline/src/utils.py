"""
utils.py – Shared helpers for the Batch File Ingestion Pipeline.

Provides:
    • Powertools Logger / Tracer singletons
    • DynamoDB batch-write & EOF marker emission helpers
    • Generic environment / SSM parameter readers
    • Resilient S3 range read & metadata retrieval
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from aws_lambda_powertools import Logger, Tracer

# ──────────────────────────────────────────────
# Observability singletons
# ──────────────────────────────────────────────
SERVICE_NAME: str = os.environ.get("POWERTOOLS_SERVICE_NAME", "file-ingestion-pipeline")

logger: Logger = Logger(service=SERVICE_NAME)
tracer: Tracer = Tracer(service=SERVICE_NAME)

# ──────────────────────────────────────────────
# AWS clients (module-level, reused across warm starts)
# ──────────────────────────────────────────────
_dynamodb_resource = None
_s3_client = None
_ssm_client = None


def get_dynamodb_resource():
    """Return cached DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def get_s3_client():
    """Return cached S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def get_ssm_client():
    """Return cached SSM client."""
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


# ──────────────────────────────────────────────
# Environment / SSM helpers
# ──────────────────────────────────────────────


def get_env(name: str, default: str | None = None) -> str:
    """Return an environment variable or raise if missing and no default."""
    value = os.environ.get(name, default)
    if value is None:
        raise EnvironmentError(f"Required environment variable '{name}' is not set")
    return value


@tracer.capture_method
def get_ssm_parameter(name: str, decrypt: bool = False) -> str:
    """Fetch a single SSM parameter value with error handling."""
    ssm = get_ssm_client()
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=decrypt)
        return response["Parameter"]["Value"]
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to retrieve SSM parameter", extra={"parameter_name": name})
        raise


# ──────────────────────────────────────────────
# S3 helpers
# ──────────────────────────────────────────────


@tracer.capture_method
def s3_head_object(bucket: str, key: str) -> Dict[str, Any]:
    """HEAD an S3 object and return its metadata dict with robust error handling."""
    s3 = get_s3_client()
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except (ClientError, BotoCoreError) as err:
        logger.exception("S3 head_object failed", extra={"bucket": bucket, "key": key})
        raise


@tracer.capture_method
def s3_get_byte_range(bucket: str, key: str, start: int, end: int) -> bytes:
    """Fetch an inclusive byte range from an S3 object."""
    s3 = get_s3_client()
    range_header = f"bytes={start}-{end}"
    logger.debug("S3 range read initiated", extra={"bucket": bucket, "key": key, "range": range_header})
    try:
        response = s3.get_object(Bucket=bucket, Key=key, Range=range_header)
        return response["Body"].read()
    except (ClientError, BotoCoreError) as err:
        logger.exception(
            "S3 range read failed",
            extra={"bucket": bucket, "key": key, "start_byte": start, "end_byte": end},
        )
        raise


# ──────────────────────────────────────────────
# DynamoDB helpers
# ──────────────────────────────────────────────


def _get_table(table_name: str):
    """Return a DynamoDB Table resource."""
    return get_dynamodb_resource().Table(table_name)


@tracer.capture_method
def batch_write_items(table_name: str, items: Sequence[Dict[str, Any]]) -> int:
    """
    Write a batch of items to a DynamoDB table using the high-level
    batch_writer (handles retries / unprocessed items internally).

    Returns the count of items written.
    """
    if not items:
        return 0

    table = _get_table(table_name)
    written = 0
    try:
        with table.batch_writer() as writer:
            for item in items:
                writer.put_item(Item=item)
                written += 1
        logger.info("Batch write complete", extra={"table": table_name, "count": written})
        return written
    except (ClientError, BotoCoreError) as err:
        logger.exception(
            "DynamoDB batch_writer failed",
            extra={"table": table_name, "attempted_count": len(items)},
        )
        raise


@tracer.capture_method
def parallel_batch_write(
    table_name: str,
    batches: List[List[Dict[str, Any]]],
    max_workers: int = 4,
) -> int:
    """
    Write multiple batches in parallel using a ThreadPoolExecutor.

    Each sub-list in *batches* is handed to `batch_write_items` in its own thread.
    Returns the total number of items written across all batches.
    """
    total_written = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(batch_write_items, table_name, batch): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                count = future.result()
                total_written += count
            except Exception:
                logger.exception(
                    "Batch write failed",
                    extra={"table": table_name, "batch_index": batch_idx},
                )
                raise

    logger.info(
        "Parallel batch write complete",
        extra={"table": table_name, "total_written": total_written, "batches": len(batches)},
    )
    return total_written


@tracer.capture_method
def emit_end_of_file_marker(
    table_name: str,
    total_records: int,
    total_chunks: int,
    status: str = "COMPLETED",
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Emit terminal End_Of_File milestone record into the raw records table.
    Enables downstream stream processing consumers to trigger next-stage jobs.
    """
    table = _get_table(table_name)
    now_iso = datetime.now(timezone.utc).isoformat()
    marker_item: Dict[str, Any] = {
        "id": "End_Of_File",
        "total_records": total_records,
        "total_chunks": total_chunks,
        "status": status,
        "completed_at": now_iso,
    }
    if metadata:
        for k, v in metadata.items():
            if k not in marker_item:
                marker_item[k] = v

    try:
        table.put_item(Item=marker_item)
        logger.info(
            "Emitted End_Of_File marker into target table",
            extra={
                "table": table_name,
                "total_records": total_records,
                "total_chunks": total_chunks,
            },
        )
        return marker_item
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to write End_Of_File marker", extra={"table": table_name})
        raise


# ──────────────────────────────────────────────
# Chunk / partitioning helpers
# ──────────────────────────────────────────────


def partition_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split *items* into sub-lists of at most *chunk_size* elements."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
