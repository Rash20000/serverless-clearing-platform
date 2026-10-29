#!/usr/bin/env python3
"""Local Ingestion & Reconciliation Pipeline Simulator.

Executes the entire batch clearing pipeline flow locally in Python without live AWS dependencies:
  1. Generates synthetic fixed-width clearing feed with header, body, and trailer.
  2. Runs Probe Service: detects row length and snaps chunk boundaries.
  3. Runs Worker Services concurrently: simulates parallel S3 HTTP Range Reads and fixed-width parsing.
  4. Runs Completion Service: performs Two-Phase Financial Reconciliation, checks control totals,
     and applies the tolerance threshold (<= 10 warning, > 10 critical breach).
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

# Add pipeline directories to sys.path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(REPO_ROOT, "file-ingestion-pipeline")
SRC_DIR = os.path.join(PIPELINE_DIR, "src")
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")

sys.path.insert(0, PIPELINE_DIR)
sys.path.insert(0, SRC_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import tests  # Initialize boto3 mocks and environment variables
from generate_clearing_feed import generate_feed
import probe_service
import worker_service
import completion_service


def run_simulation(num_records: int = 5000, inject_errors: int = 0, rows_per_chunk: int = 1000):
    print("=" * 75)
    print(f"STARTING CLEARING PIPELINE SIMULATION")
    print(f"Records: {num_records} | Injected Errors: {inject_errors} | Chunk Size: {rows_per_chunk} rows")
    print("=" * 75)

    # 1. Generate Synthetic Feed in Memory
    feed_text = generate_feed(num_records=num_records, inject_errors=inject_errors)
    feed_bytes = feed_text.encode("latin-1")
    total_size = len(feed_bytes)
    print(f"\n[1. Ingestion Landing] Feed deposited in memory ({total_size:,} bytes)")

    # 2. Phase 1: Probe Service
    sample_header = feed_bytes[:2048]
    row_length = probe_service.detect_row_length(sample_header)
    print(f"[2. Probe Phase] Detected row length: {row_length} bytes (including delimiter)")

    manifests = probe_service.build_chunk_manifests(
        bucket="institutional-landing-bucket",
        key="inbound/clearing/daily_trades.dat",
        total_size=total_size,
        row_length=row_length,
        rows_per_chunk=rows_per_chunk,
    )
    print(f"[2. Probe Phase] Partitioned into {len(manifests)} non-overlapping, row-aligned chunks:")
    for idx, m in enumerate(manifests[:3]):
        print(f"    Chunk {idx}: bytes {m['start_byte']:,} to {m['end_byte']:,} (rows {m['start_row']:,} - {m['end_row']:,})")
    if len(manifests) > 3:
        print(f"    ... and {len(manifests) - 3} more chunks.")

    # 3. Phase 2: Distributed Map Workers (Parallel HTTP Range Reads & Parsing)
    print(f"\n[3. Worker Phase] Dispatching {len(manifests)} workers across ThreadPoolExecutor...")

    def execute_worker(chunk: dict) -> dict:
        # Simulate S3 HTTP Range Read: bytes=start_byte-end_byte
        chunk_raw = feed_bytes[chunk["start_byte"] : chunk["end_byte"] + 1]
        lines = worker_service.decode_and_split(chunk_raw)
        records = worker_service.parallel_parse(lines)
        chunk_failed = max(0, len(lines) - len(records))
        chunk_amount = sum(
            float(r.get("amount", 0) or 0)
            for r in records
            if str(r.get("amount", "")).replace(".", "", 1).isdigit()
        )
        return {
            "status": "SUCCESS",
            "start_row": chunk["start_row"],
            "end_row": chunk["end_row"],
            "records_parsed": len(records),
            "records_written": len(records),
            "records_failed": chunk_failed,
            "amount_sum": round(chunk_amount, 2),
        }

    map_results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for res in executor.map(execute_worker, manifests):
            map_results.append(res)

    print(f"[3. Worker Phase] All {len(map_results)} workers completed successfully.")

    # 4. Phase 3: Completion Service & Two-Phase Financial Reconciliation
    print(f"\n[4. Completion & Reconciliation Phase]")
    stats = completion_service.aggregate_chunk_results(map_results)
    print(f"    Total Records Parsed:  {stats['total_records_parsed']:,}")
    print(f"    Total Records Written: {stats['total_records_written']:,}")
    print(f"    Total Records Failed:  {stats['total_records_failed']:,}")
    print(f"    Total Amount Volume:   ${stats['total_amount_sum']:,.2f}")

    probe_result = {
        "bucket": "institutional-landing-bucket",
        "key": "inbound/clearing/daily_trades.dat",
        "total_size": total_size,
        "total_chunks": len(manifests),
        "expected_records": num_records,
    }

    recon = completion_service.reconcile_feed_control_totals(stats, probe_result, tolerance_threshold=10)

    print("\n" + "-" * 75)
    print(f"RECONCILIATION RESULT: [{recon['reconciliation_status']}] -> Pipeline Status: [{recon['status']}]")
    print(f"Variance Count: {recon['count_variance']} (Tolerance Threshold: {recon['tolerance_threshold']})")

    if recon["reconciliation_status"] == "BALANCED":
        print(">> PASS: 100% balance parity verified. Terminal 'End_Of_File' milestone emitted with status COMPLETED.")
        print(">> Downstream reporting and enrichment jobs triggered.")
    elif recon["reconciliation_status"] == "WARNING_WITHIN_TOLERANCE":
        print(f">> WARNING: Minor variance detected ({recon['count_variance']} failed records <= {recon['tolerance_threshold']}).")
        print(">> Acceptable fault: Batch marked COMPLETED_WITH_WARNINGS for support review via DLQ.")
        print(">> Downstream reporting permitted to proceed.")
    else:
        print(f">> CRITICAL BREACH: Variance ({recon['count_variance']}) exceeds threshold ({recon['tolerance_threshold']})!")
        print(">> Step Functions execution HALTED. Sev-1 P1 CloudWatch alarm fired to Operations.")
        print(">> Downstream reporting strictly blocked to prevent out-of-balance settlement.")
    print("-" * 75)


def main():
    parser = argparse.ArgumentParser(description="Simulate local ingestion, range reads, and reconciliation.")
    parser.add_argument("-n", "--records", type=int, default=5000, help="Number of records (default: 5000)")
    parser.add_argument("-e", "--inject-errors", type=int, default=0, help="Injected malformed records (default: 0)")
    parser.add_argument("-c", "--chunk-size", type=int, default=1000, help="Rows per chunk (default: 1000)")

    args = parser.parse_args()
    run_simulation(num_records=args.records, inject_errors=args.inject_errors, rows_per_chunk=args.chunk_size)


if __name__ == "__main__":
    main()
