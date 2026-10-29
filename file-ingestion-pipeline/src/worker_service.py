"""
worker_service.py – Worker Lambda for the Batch File Ingestion Pipeline.

Responsibilities:
    1. Receive a single chunk descriptor from Step Functions Distributed Map.
    2. Fetch the exact byte range from S3 via Range Read.
    3. Decode bytes → split lines → parse fixed-width fields (pure functions).
    4. Batch-write parsed records into the target DynamoDB table, using
       parallel ThreadPoolExecutor batches for throughput.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from botocore.exceptions import BotoCoreError, ClientError
from utils import (
    batch_write_items,
    get_env,
    logger,
    partition_list,
    s3_get_byte_range,
    tracer,
)

# ──────────────────────────────────────────────
# Fixed-width field layout
# ──────────────────────────────────────────────
# Layout matches generic high-volume ledger / raw feed:
# "ACC1234567JOHN_DOE_CORP                 ACTIVE    000000150000"
#  id[0:10]  entity_name[10:40]            status[40:50]  amount[50:62]


@dataclass(frozen=True, slots=True)
class FieldSlice:
    """Describes one fixed-width column: name, start offset, end offset."""
    name: str
    start: int
    end: int


FIELD_LAYOUT: tuple[FieldSlice, ...] = (
    FieldSlice("id",          0,  10),
    FieldSlice("entity_name", 10, 40),
    FieldSlice("status",      40, 50),
    FieldSlice("amount",      50, 62),
)

DDB_BATCH_SIZE: int = 25
MAX_WRITE_WORKERS: int = 4
MAX_PARSE_WORKERS: int = 8


# ──────────────────────────────────────────────
# Pure parsing functions
# ──────────────────────────────────────────────


def parse_line(line: str) -> Dict[str, str]:
    """
    Parse a single fixed-width line into a dict using FIELD_LAYOUT.
    All values are stripped of trailing whitespace.
    """
    if not line:
        return {}
    return {field.name: line[field.start : field.end].strip() for field in FIELD_LAYOUT}


def parse_lines(lines: List[str]) -> List[Dict[str, str]]:
    """Parse a batch of lines sequentially (used inside each thread worker)."""
    parsed: List[Dict[str, str]] = []
    for line in lines:
        if line.strip():
            record = parse_line(line)
            rec_id = record.get("id", "")
            # Filter out header and trailer control records from the data ledger
            if rec_id and not rec_id.startswith(("HDR", "TRL", "HEADER", "TRAILER")):
                parsed.append(record)
    return parsed


def decode_and_split(raw: bytes) -> List[str]:
    """Decode raw bytes and split into individual lines, discarding blanks."""
    if not raw:
        return []
    text = raw.decode("latin-1")
    return [ln for ln in text.split("\n") if ln.strip()]


# ──────────────────────────────────────────────
# Parallel parsing
# ──────────────────────────────────────────────


@tracer.capture_method
def parallel_parse(lines: List[str], max_workers: int = MAX_PARSE_WORKERS) -> List[Dict[str, str]]:
    """
    Distribute line parsing across a ThreadPoolExecutor.
    Sub-lists are parsed concurrently and flattened preserving original row order.
    """
    if not lines:
        return []

    chunk_size = max(1, len(lines) // max_workers)
    sub_lists = partition_list(lines, chunk_size)
    results: List[List[Dict[str, str]]] = [[] for _ in sub_lists]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(parse_lines, sub): idx
            for idx, sub in enumerate(sub_lists)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            results[idx] = future.result()

    return [record for batch in results for record in batch]


# ──────────────────────────────────────────────
# Parallel DynamoDB writing
# ──────────────────────────────────────────────


@tracer.capture_method
def parallel_write(
    table_name: str,
    records: List[Dict[str, str]],
    batch_size: int = DDB_BATCH_SIZE,
    max_workers: int = MAX_WRITE_WORKERS,
) -> int:
    """
    Partition records into sub-batches of 25 and write them to DynamoDB
    in parallel via ThreadPoolExecutor.
    """
    if not records:
        return 0

    batches = partition_list(records, batch_size)
    total = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(batch_write_items, table_name, batch): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                total += future.result()
            except (ClientError, BotoCoreError) as err:
                logger.exception(
                    "DynamoDB batch write failed",
                    extra={"table": table_name, "batch_index": batch_idx},
                )
                raise

    return total


# ──────────────────────────────────────────────
# Lambda handler
# ──────────────────────────────────────────────


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Entry-point for the Worker Lambda.

    Expected event keys (supplied by Step Functions Distributed Map):
        bucket      – S3 bucket name
        key         – S3 object key
        start_byte  – inclusive start byte
        end_byte    – inclusive end byte
        start_row   – first row index
        end_row     – last row index
    """
    for required in ("bucket", "key", "start_byte", "end_byte", "start_row", "end_row"):
        if required not in event:
            raise KeyError(f"Missing required parameter '{required}' in worker event payload")

    bucket: str = str(event["bucket"])
    key: str = str(event["key"])
    start_byte: int = int(event["start_byte"])
    end_byte: int = int(event["end_byte"])
    start_row: int = int(event["start_row"])
    end_row: int = int(event["end_row"])

    table_name: str = get_env("TARGET_TABLE_NAME")

    logger.info(
        "Processing chunk range",
        extra={
            "bucket": bucket,
            "key": key,
            "start_byte": start_byte,
            "end_byte": end_byte,
            "start_row": start_row,
            "end_row": end_row,
        },
    )

    # 1. Fetch byte range from S3
    raw = s3_get_byte_range(bucket, key, start_byte, end_byte)
    logger.info("Fetched byte range from S3", extra={"bytes_received": len(raw)})

    # 2. Decode → split → parse in parallel
    lines = decode_and_split(raw)
    records = parallel_parse(lines)
    logger.info("Parsed chunk records", extra={"line_count": len(lines), "record_count": len(records)})

    # Calculate chunk financial aggregates & failed row variance
    chunk_failed_records = max(0, len(lines) - len(records))
    chunk_amount_sum = sum(
        float(r.get("amount", 0) or 0)
        for r in records
        if str(r.get("amount", "")).replace(".", "", 1).isdigit()
    )

    # 3. Write to DynamoDB in parallel batches
    written = parallel_write(table_name, records)
    logger.info("Chunk processing complete", extra={"written": written, "parsed": len(records), "failed": chunk_failed_records, "amount_sum": chunk_amount_sum})

    return {
        "status": "SUCCESS",
        "start_row": start_row,
        "end_row": end_row,
        "records_parsed": len(records),
        "records_written": written,
        "records_failed": chunk_failed_records,
        "amount_sum": round(chunk_amount_sum, 2),
    }


# Backwards compatibility alias
handler = lambda_handler

