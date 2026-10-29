"""
probe_service.py – Probe Lambda for the Batch File Ingestion Pipeline.

Responsibilities:
    1. Triggered via S3 ObjectCreated event on landing bucket at inbound/feeds/.
    2. HEAD the S3 object to get total file size.
    3. Read byte range 0-2048 to detect exact row byte length (incl. newline).
    4. Divide the file into N-row chunks and emit a JSON array of manifests.
    5. Starts Step Functions Distributed Map execution to process chunks via worker_service.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from typing import Any, Dict, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from utils import (
    get_env,
    logger,
    s3_get_byte_range,
    s3_head_object,
    tracer,
)

PROBE_BYTES: int = 2048
DEFAULT_ROWS_PER_CHUNK: int = 5_000

_sfn_client = None


def get_sfn_client():
    """Return cached Step Functions client."""
    global _sfn_client
    if _sfn_client is None:
        _sfn_client = boto3.client("stepfunctions")
    return _sfn_client


def detect_row_length(sample: bytes) -> int:
    """
    Determine the byte length of a single fixed-width row from a raw sample.
    Accounts for both LF and CRLF line terminators.
    """
    if not sample:
        raise ValueError("Cannot detect row length from empty sample")

    text = sample.decode("latin-1")
    lines = text.split("\n")
    if len(lines) < 2:
        raise ValueError("Sample too small to detect row boundary – no newline found")

    first_line_length = len(lines[0]) + 1  # includes the '\n' byte
    logger.info("Detected row length", extra={"row_bytes": first_line_length})
    return first_line_length


def _snap_to_row_boundary(end_byte: int, row_length: int, total_size: int) -> int:
    """Ensure end_byte falls exactly at the last byte of a complete row."""
    if row_length <= 0:
        raise ValueError(f"row_length must be positive, got {row_length}")
    if end_byte >= total_size - 1:
        return total_size - 1
    rows_covered = (end_byte + 1) // row_length
    snapped = rows_covered * row_length - 1
    return max(snapped, 0)


def build_chunk_manifests(
    bucket: str,
    key: str,
    total_size: int,
    row_length: int,
    rows_per_chunk: int,
) -> List[Dict[str, Any]]:
    """
    Slice the file into row-aligned byte ranges.
    Each manifest specifies byte offsets and row indices for a single worker chunk.
    """
    if total_size <= 0:
        return []
    if row_length <= 0 or rows_per_chunk <= 0:
        raise ValueError("row_length and rows_per_chunk must be positive")

    chunk_bytes = rows_per_chunk * row_length
    manifests: List[Dict[str, Any]] = []

    start_byte = 0
    row_idx = 0

    while start_byte < total_size:
        raw_end = min(start_byte + chunk_bytes - 1, total_size - 1)
        end_byte = _snap_to_row_boundary(raw_end, row_length, total_size)
        rows_in_chunk = (end_byte - start_byte + 1) // row_length

        manifests.append(
            {
                "bucket": bucket,
                "key": key,
                "start_byte": start_byte,
                "end_byte": end_byte,
                "start_row": row_idx,
                "end_row": row_idx + max(0, rows_in_chunk - 1),
            }
        )
        start_byte = end_byte + 1
        row_idx += rows_in_chunk

    return manifests


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Entry-point for the Probe Lambda.
    Handles direct invocation or S3 event notification from landing bucket upload.
    """
    bucket: str = ""
    key: str = ""

    # Parse S3 event notification if present
    if "Records" in event and event["Records"] and "s3" in event["Records"][0]:
        s3_record = event["Records"][0]["s3"]
        bucket = s3_record["bucket"]["name"]
        raw_key = s3_record["object"]["key"]
        key = urllib.parse.unquote_plus(raw_key)
        logger.info("Parsed S3 event notification", extra={"bucket": bucket, "key": key})
    else:
        bucket = event.get("bucket") or get_env("LOAD_BUCKET")
        key = event.get("key") or get_env("LOAD_KEY", "")

    if not bucket or not key:
        raise ValueError("Both 'bucket' and 'key' must be provided via event or environment variables")

    rows_per_chunk: int = int(
        event.get("rows_per_chunk", get_env("ROWS_PER_CHUNK", str(DEFAULT_ROWS_PER_CHUNK)))
    )

    state_machine_arn: str = event.get("state_machine_arn") or os.environ.get("STATE_MACHINE_ARN", "")

    # 1. HEAD to get total file size
    head = s3_head_object(bucket, key)
    total_size: int = head["ContentLength"]
    logger.info("File size determined", extra={"bucket": bucket, "key": key, "total_size": total_size})

    if total_size == 0:
        logger.warning("Empty file detected; aborting chunk generation", extra={"bucket": bucket, "key": key})
        return {
            "bucket": bucket,
            "key": key,
            "total_size": 0,
            "row_length": 0,
            "rows_per_chunk": rows_per_chunk,
            "total_chunks": 0,
            "chunks": [],
        }

    # 2. Read sample to detect row length
    sample_bytes = min(PROBE_BYTES - 1, total_size - 1)
    sample = s3_get_byte_range(bucket, key, 0, sample_bytes)
    row_length = detect_row_length(sample)

    # 3. Build chunk manifests
    manifests = build_chunk_manifests(bucket, key, total_size, row_length, rows_per_chunk)

    result = {
        "bucket": bucket,
        "key": key,
        "total_size": total_size,
        "row_length": row_length,
        "rows_per_chunk": rows_per_chunk,
        "total_chunks": len(manifests),
        "chunks": manifests,
    }

    # 4. Trigger Step Functions execution if configured and triggered via S3 event or flag
    if state_machine_arn and ("Records" in event or event.get("trigger_execution")):
        req_id = getattr(context, "aws_request_id", "local-run")[:8]
        exec_name = f"file-load-{req_id}"
        logger.info(
            "Starting Step Functions execution",
            extra={"state_machine": state_machine_arn, "execution_name": exec_name},
        )
        sfn = get_sfn_client()
        try:
            sfn.start_execution(
                stateMachineArn=state_machine_arn,
                name=exec_name,
                input=json.dumps({"probe_result": result}),
            )
        except (ClientError, BotoCoreError) as err:
            logger.exception("Failed to trigger Step Functions execution", extra={"state_machine": state_machine_arn})
            raise

    return result


# Backwards compatibility alias
handler = lambda_handler
