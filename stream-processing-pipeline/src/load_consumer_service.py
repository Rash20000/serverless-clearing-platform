"""
Load Service — DynamoDB Streams Consumer for upstream raw records table.

Listens to stream events from the upstream raw records table.
Processes and enriches records, transforms them into `cache.report` items,
and watches specifically for the completion marker `pk = "End_Of_File"`.

When `pk == "End_Of_File"` is received (signifying the ingestion pipeline has finished):
- Completes pending batch writes to cache.report
- Records `{function_name}-end` (e.g. `load-end`) into the DynamoDB `jobs` table
  so that the downstream Job Watcher can detect load completion and trigger the Report Job.

Environment variables:
    REFERENCE_TABLE_NAME    — DynamoDB reference table for record enrichment
    CACHE_REPORT_TABLE_NAME  — DynamoDB table for report cache
    JOBS_TABLE_NAME         — DynamoDB table for job state tracking
    JOB_PREFIX              — Partition key prefix for cache items (default: job-x)
    FUNCTION_NAME           — Lambda logical name for status record (default: load)
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List

from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

from utils import (
    batch_write_items,
    get_current_date_string,
    get_dynamodb_resource,
    logger,
    query_all_items,
    record_job_end,
    tracer,
)

_DESERIALIZER = TypeDeserializer()

REFERENCE_TABLE_NAME: str = os.environ.get("REFERENCE_TABLE_NAME", "")
CACHE_REPORT_TABLE_NAME: str = os.environ.get("CACHE_REPORT_TABLE_NAME", "")
JOBS_TABLE_NAME: str = os.environ.get("JOBS_TABLE_NAME", "")
JOB_PREFIX: str = os.environ.get("JOB_PREFIX", "job-x")
SERVICE_FUNCTION_NAME: str = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "load")

_ACCEPTED_EVENT_NAMES = frozenset({"INSERT", "MODIFY"})
_END_OF_FILE_KEY = "End_Of_File"
_MAX_WORKERS = 10


# ---------------------------------------------------------------------------
# Stream helpers
# ---------------------------------------------------------------------------

@tracer.capture_method
def deserialize_stream_record(record: dict) -> dict:
    """Extract and deserialize the NewImage from a DynamoDB stream record."""
    try:
        new_image: Dict[str, Any] = record["dynamodb"]["NewImage"]
    except KeyError:
        logger.warning(
            "Stream record missing NewImage",
            extra={"eventID": record.get("eventID")},
        )
        return {}

    return {
        key: _DESERIALIZER.deserialize(value)
        for key, value in new_image.items()
    }


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

@tracer.capture_method
def enrich_record(record: dict) -> dict:
    """
    Look up reference data from REFERENCE_TABLE_NAME and merge into record.
    Matches PK = record['id'] and SK begins_with('REF#').
    """
    record_id: str = str(record.get("id") or record.get("pk") or record.get("PK", ""))
    if not record_id or record_id == _END_OF_FILE_KEY:
        return record

    if not REFERENCE_TABLE_NAME:
        return record

    try:
        dynamodb = get_dynamodb_resource()
        table = dynamodb.Table(REFERENCE_TABLE_NAME)
        key_condition = Key("PK").eq(record_id) & Key("SK").begins_with("REF#")
        ref_items = query_all_items(table=table, key_condition_expression=key_condition)

        enriched = dict(record)
        for item in ref_items:
            enriched = {**item, **enriched}
        return enriched
    except Exception:
        logger.exception("Reference enrichment failed; using base record", extra={"record_id": record_id})
        return record


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

@tracer.capture_method
def transform_to_cache_item(record: dict) -> dict:
    """
    Transform record into cache.report schema:
        PK: {JOB_PREFIX}#{hash_or_last_char_of_id} (e.g. job-x#A)
        SK: {date_string}#{id} (e.g. 20261018#CUSIP9981A)
    """
    record_id = str(record.get("id") or record.get("pk") or record.get("PK", ""))
    last_char = record_id[-1].upper() if record_id else "X"
    date_string = get_current_date_string()

    return {
        **record,
        "PK": f"{JOB_PREFIX}#{last_char}",
        "SK": f"{date_string}#{record_id}",
        "id": record_id,
        "status": record.get("status", "ACTIVE"),
        "amount": str(record.get("amount", "0")),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: Any) -> dict:
    """
    Process DynamoDB Stream records from upstream source table.
    Enriches normal records and writes to cache.report.
    Detects pk = "End_Of_File" marker, completes processing, and writes `load-end` into jobs table.
    Supports partial batch item failure reporting (ReportBatchItemFailures) for poison-message isolation.
    """
    records: List[dict] = event.get("Records", [])
    logger.info("Received stream batch from upstream table", extra={"record_count": len(records)})

    batch_item_failures: List[Dict[str, str]] = []
    data_records: List[dict] = []
    end_of_file_detected = False
    eof_metadata: dict[str, Any] = {}

    for r in records:
        event_name = r.get("eventName", "")
        if event_name not in _ACCEPTED_EVENT_NAMES:
            continue

        try:
            item = deserialize_stream_record(r)
        except Exception:
            sequence_number = r.get("dynamodb", {}).get("SequenceNumber")
            if sequence_number:
                logger.exception("Failed to deserialize stream record NewImage", extra={"sequence_number": sequence_number})
                batch_item_failures.append({"itemIdentifier": sequence_number})
            continue

        if not item:
            continue

        # Check for pk == "End_Of_File"
        pk_val = item.get("pk") or item.get("PK") or item.get("id")
        if pk_val == _END_OF_FILE_KEY:
            logger.info("Received End_Of_File marker record from stream", extra={"item": item})
            end_of_file_detected = True
            eof_metadata = item
        else:
            data_records.append(item)

    # 1. Enrich data records in parallel
    if data_records:
        enriched_records: List[dict] = []
        with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(data_records))) as executor:
            future_to_rec = {executor.submit(enrich_record, rec): rec for rec in data_records}
            for fut in as_completed(future_to_rec):
                try:
                    enriched_records.append(fut.result())
                except Exception:
                    logger.exception("Failed to enrich record", extra={"rec": future_to_rec[fut]})

        # 2. Write to cache.report
        if enriched_records and CACHE_REPORT_TABLE_NAME:
            cache_items = [transform_to_cache_item(r) for r in enriched_records]
            batch_write_items(CACHE_REPORT_TABLE_NAME, cache_items)
            logger.info("Cached report records", extra={"count": len(cache_items)})

    # 3. Handle End_Of_File marker
    if end_of_file_detected:
        logger.info(
            "End_Of_File detected. Recording load-end completion in jobs table.",
            extra={"jobs_table": JOBS_TABLE_NAME},
        )
        record_job_end(
            jobs_table_name=JOBS_TABLE_NAME,
            function_name="load",
            status="COMPLETED",
            metadata={
                "marker": _END_OF_FILE_KEY,
                "processed_records_in_final_batch": len(data_records),
                "file_metadata": eof_metadata,
            },
        )

    return {
        "statusCode": 200,
        "processed_records": len(data_records),
        "end_of_file_detected": end_of_file_detected,
        "batchItemFailures": batch_item_failures,
    }


# Backwards compatibility alias
handler = lambda_handler

