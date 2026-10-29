"""
completion_service.py – Completion Lambda for the Batch File Ingestion Pipeline.

Responsibilities:
    1. Executed as the terminal step in the Step Functions Distributed Map workflow.
    2. Aggregates chunk execution outputs (records parsed, records written).
    3. Emits terminal End_Of_File milestone record into the raw records DynamoDB table.
    4. Triggers downstream DynamoDB Streams consumer to initiate the reporting workflow.
"""

from __future__ import annotations

from typing import Any, Dict, List

from utils import (
    emit_end_of_file_marker,
    get_env,
    logger,
    tracer,
)


def aggregate_chunk_results(map_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Sum total records parsed, written, failed, and amount sum across all worker chunk results.
    Filters out any non-dictionary results defensively.
    """
    total_written = 0
    total_parsed = 0
    total_failed = 0
    total_amount = 0.0

    for res in map_results:
        if isinstance(res, dict):
            total_written += int(res.get("records_written", 0))
            total_parsed += int(res.get("records_parsed", 0))
            total_failed += int(res.get("records_failed", 0))
            total_amount += float(res.get("amount_sum", 0.0))

    return {
        "total_records_written": total_written,
        "total_records_parsed": total_parsed,
        "total_records_failed": total_failed,
        "total_amount_sum": round(total_amount, 2),
        "total_chunks": len(map_results),
    }


def reconcile_feed_control_totals(
    stats: Dict[str, Any],
    probe_result: Dict[str, Any],
    tolerance_threshold: int = 10,
) -> Dict[str, Any]:
    """
    Two-Phase Financial Reconciliation:
    Validates aggregated chunk worker records and amount sums against feed trailer control totals.
    
    Tolerance Strategy:
      - 0 failures & exact count: BALANCED (Status: COMPLETED)
      - <= 10 failures: WARNING_WITHIN_TOLERANCE (Status: COMPLETED_WITH_WARNINGS)
        Acceptable fault for production support to investigate via DLQ/logs while allowing downstream jobs.
      - > 10 failures or critical mismatch: CRITICAL_OUT_OF_BALANCE (Status: FAILED)
        Halts downstream pipeline and emits Sev-1 alarm.
    """
    total_written = stats.get("total_records_written", 0)
    total_failed = stats.get("total_records_failed", 0)
    total_amount = stats.get("total_amount_sum", 0.0)

    expected_records = probe_result.get("expected_records")
    expected_amount = probe_result.get("expected_amount")

    count_variance = total_failed
    if expected_records is not None:
        count_variance = abs(int(expected_records) - total_written)

    amount_variance = 0.0
    if expected_amount is not None:
        amount_variance = abs(float(expected_amount) - total_amount)

    if count_variance == 0 and (expected_amount is None or amount_variance < 0.01):
        reconciliation_status = "BALANCED"
        status = "COMPLETED"
    elif count_variance <= tolerance_threshold and (expected_amount is None or amount_variance < 100.0):
        reconciliation_status = "WARNING_WITHIN_TOLERANCE"
        status = "COMPLETED_WITH_WARNINGS"
    else:
        reconciliation_status = "CRITICAL_OUT_OF_BALANCE"
        status = "FAILED"

    return {
        "status": status,
        "reconciliation_status": reconciliation_status,
        "total_records_written": total_written,
        "total_records_failed": total_failed,
        "expected_records": expected_records,
        "count_variance": count_variance,
        "total_amount_sum": total_amount,
        "expected_amount": expected_amount,
        "amount_variance": round(amount_variance, 2),
        "tolerance_threshold": tolerance_threshold,
    }


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Entry-point for the Completion Lambda.

    Expected event shape from Step Functions:
        probe_result: metadata from probe_service (bucket, key, total_size, total_chunks, expected_records, expected_amount)
        map_results: list of chunk execution results from worker_service
    """
    probe_result: Dict[str, Any] = event.get("probe_result", {})
    map_results: List[Dict[str, Any]] = event.get("map_results", [])

    table_name: str = get_env("TARGET_TABLE_NAME")
    bucket: str = probe_result.get("bucket", "")
    key: str = probe_result.get("key", "")
    total_size: int = int(probe_result.get("total_size", 0))

    logger.info(
        "Ingestion completed; aggregating chunk results and reconciling control totals",
        extra={
            "bucket": bucket,
            "key": key,
            "chunk_count": len(map_results),
            "table_name": table_name,
        },
    )

    stats = aggregate_chunk_results(map_results)
    recon = reconcile_feed_control_totals(stats, probe_result, tolerance_threshold=10)

    total_written = stats["total_records_written"]
    total_parsed = stats["total_records_parsed"]
    total_chunks = stats["total_chunks"]
    final_status = recon["status"]

    if recon["reconciliation_status"] == "CRITICAL_OUT_OF_BALANCE":
        logger.error(
            "FATAL: Ingestion control total reconciliation failed; variance exceeds fault threshold",
            extra={"reconciliation": recon},
        )
    elif recon["reconciliation_status"] == "WARNING_WITHIN_TOLERANCE":
        logger.warning(
            "Ingestion completed with minor record variance within acceptable tolerance (<= 10); alerting support",
            extra={"reconciliation": recon},
        )
    else:
        logger.info("Ingestion completed with 100% balance reconciliation parity", extra={"reconciliation": recon})

    # Emit the terminal End_Of_File marker into DynamoDB table with reconciliation status
    marker = emit_end_of_file_marker(
        table_name=table_name,
        total_records=total_written,
        total_chunks=total_chunks,
        status=final_status,
        metadata={
            "source_bucket": bucket,
            "source_key": key,
            "file_size_bytes": total_size,
            "reconciliation": recon,
        },
    )

    if final_status == "FAILED":
        raise ValueError(
            f"Ingestion batch halted: critical reconciliation breach ({recon['count_variance']} records > {recon['tolerance_threshold']})"
        )

    return {
        "status": final_status,
        "reconciliation": recon,
        "total_records_written": total_written,
        "total_records_parsed": total_parsed,
        "total_chunks": total_chunks,
        "source_bucket": bucket,
        "source_key": key,
        "marker": marker,
    }


# Backwards compatibility alias
handler = lambda_handler

