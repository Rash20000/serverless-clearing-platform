# Batch File Ingestion Pipeline (`file-ingestion-pipeline`)

[← Back to Platform Architecture](../README.md)

> **Subsystem Scope**: High-throughput distributed batch ingestion for multi-gigabyte flat files via S3 range reads and Step Functions.
> Responsible for high-throughput batch clearing feed ingestion, byte-accurate row boundary calculation, and parallel DynamoDB ingestion without row splitting or Lambda execution timeouts.

---

## Subsystem Interconnections
- **Upstream Trigger**: Ingests multi-gigabyte flat files deposited in landing buckets managed via [`shared-infra`](../shared-infra/README.md).
- **Downstream Consumer**: Writes raw records into the primary ledger, whose DynamoDB Streams feed [`stream-processing-pipeline`](../stream-processing-pipeline/README.md).
- **Milestone Choreography**: Dispatches an `End_Of_File` milestone to signal batch completion to [`job-service`](../job-service/README.md).

---

## 1. Production Context & Operational Mission

At the close of trading, clearing counterparties and external execution venues deposit multi-gigabyte fixed-width ledger files into S3 (`inbound/feeds/`).

Processing these feeds natively in serverless presents severe operational challenges:
- **File Sizing**: Typical files range from 5 GB to 70 GB (millions of records). Downloading the entire file into a Lambda execution environment causes disk/memory exhaustion.
- **Row Splitting Risk**: Slicing files arbitrarily across byte offsets splits multi-byte records in half, producing corrupted accounting ledgers upon insertion.
- **SLA Deadline**: All ledger lines must be loaded, validated, and persisted into the raw records DynamoDB table within a 15-minute processing window to trigger downstream settlement workflows on schedule.

---

## 2. Engineering Tradeoffs & Architectural Strategy

### A. The Probe Pattern & Row-Aligned Byte Snapping
Rather than streaming the file through a monolithic worker, the pipeline implements a **two-phase architecture**:

1. **Probe Phase (`probe_service.py`)**:
   - Triggered by `s3:ObjectCreated:*` on the landing bucket.
   - Issues an initial lightweight HTTP Range Read (`bytes=0-2047`) via `s3:GetObject` to inspect the file header.
   - Detects the line delimiter (`\r\n` or `\n`) and calculates the fixed row byte length ($L$).
   - Mathematically partitions the total file size into exact, non-overlapping byte intervals:
     $$\text{EndByte} = \min\left(\left\lfloor \frac{\text{RawEnd} + 1}{L} \right\rfloor \times L - 1, \; \text{TotalSize} - 1\right)$$
     ```python
     # Row-boundary snap implementation (src/probe_service.py)
     end_byte = min(((raw_end + 1) // row_length) * row_length - 1, total_size - 1)
     ```
   - Generates chunk manifests with guaranteed row alignment: zero split records across chunk boundaries.

2. **Distributed Map Worker Phase (`worker_service.py`)**:
   - AWS Step Functions iterates over the chunk manifests in `DISTRIBUTED` Map mode (concurrency up to 40 workers).
   - Each worker performs an isolated HTTP Range Read (`bytes=start-end`), fetching strictly its slice.
   - Slices and decodes rows using a bounded `ThreadPoolExecutor`, batch-writing records into DynamoDB in 25-item chunks (`BatchWriteItem`).

3. **Terminal Milestone Signaling (`completion_service.py`)**:
   - When all distributed chunks complete, the completion Lambda inserts a terminal milestone record:
     ```json
     {"id": "End_Of_File", "total_records": 1000000, "status": "COMPLETED"}
     ```
   - This marker is picked up downstream by `stream-processing-pipeline` over DynamoDB Streams, initiating report generation.

---

## 3. Operational Constraints & SLA Targets

| Operational Parameter | Production Target | Design Justification |
|---|---|---|
| **Rows per Chunk** | `5,000` rows | Bounds worker memory footprint to ~256 MB and execution time to <45 seconds. |
| **Distributed Map Concurrency** | `40` concurrent workers | Maximizes throughput while respecting DynamoDB write capacity unit (WCU) limits. |
| **Byte Alignment Precision** | 100% exact byte alignment | Eliminates split-row corruption during parallel range reads. |
| **Retry & Failure Handling** | 3 attempts with exponential backoff | Step Functions handles transient S3/DynamoDB throttles automatically. |
| **Idempotency** | Natural key overwrites | Re-running a failed chunk overwrites identical record IDs safely without duplication. |

---

## 4. Repository Structure

```
file-ingestion-pipeline/
├── src/
│   ├── probe_service.py           # Inspects headers, calculates row lengths, emits chunk intervals
│   ├── worker_service.py          # HTTP range-reader, fixed-width parser, parallel DynamoDB writer
│   ├── completion_service.py      # Aggregates chunk executions and writes terminal End_Of_File marker
│   └── utils.py                   # S3 range reads, chunk partitioning, batch-write helpers
├── tests/
│   ├── __init__.py                # Local test setup & mock boto3 clients
│   ├── test_probe_service.py      # Tests for row length detection, edge boundaries, malformed headers
│   ├── test_worker_service.py     # Tests for parallel range parsing, line splitting, DynamoDB batching
│   ├── test_completion_service.py # Tests for aggregation logic and End_Of_File marker emission
│   └── test_utils.py              # Tests for S3 byte range fetching and partition math
├── template.yaml                  # SAM infrastructure (Step Functions Distributed Map, Lambdas, S3)
├── build.yaml                     # AWS CodeBuild CI/CD specification
├── diagram.md                     # Sequence and state machine architectural diagrams
└── readme.md                      # Subsystem documentation
```

---

## 5. Local Verification & Unit Testing

All unit tests execute with standard library `unittest` with zero live AWS dependencies:
```bash
python -m unittest discover tests -v
```
