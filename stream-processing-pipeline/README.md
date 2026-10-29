# Stream Processing & Reporting Engine (`stream-processing-pipeline`)

[← Back to Platform Architecture](../README.md)

> **Subsystem Scope**: Real-time CDC stream processing, reference data enrichment, sharded cache population, and Pinpoint report delivery.
> Responsible for near-real-time CDC stream enrichment, 10-second batch windowing, dead-letter queue poison-pill isolation, and sharded CSV report generation.

---

## Subsystem Interconnections
- **Upstream Producer**: Consumes raw records emitted via DynamoDB Streams from [`file-ingestion-pipeline`](../file-ingestion-pipeline/README.md).
- **Dynamic Stream Discovery**: Uses `Custom::StreamArnFetcher` from [`shared-utils`](../shared-utils/README.md) to discover dynamic DynamoDB stream ARNs at deploy time.
- **Milestone Choreography**: Dispatches `load-end` and `report-end` events to [`job-service`](../job-service/README.md).
- **Downstream Consumer**: Populates `cache.report` queried by [`core-data-api`](../core-data-api/README.md).
- **Shared Storage & Layers**: Emits CSV archives to S3 buckets in [`shared-infra`](../shared-infra/README.md) and leverages ARM64 analytical runtimes from [`lambda-infra-layer`](../lambda-infra-layer/README.md).

---

## 1. Production Context & Operational Mission

As raw clearing records are persisted into the raw records table by the upstream ingestion engine, downstream financial accounting systems require enriched entity caches and consolidated daily CSV summaries.

### The Operational Challenge
- **Micro-Burst Ingestion & Throttling**: Raw records arrive in bursts of thousands of records per second. Firing a Lambda on every individual shard record overwhelms downstream reference tables with read requests.
- **Poison-Pill Records**: A single corrupted record in a DynamoDB stream batch can cause the entire batch to fail. Without partial batch failure handling, AWS retries the entire shard indefinitely, blocking subsequent records.
- **Reporting Across Large Datasets**: Scanning millions of records for daily reporting causes DynamoDB hot-partition throttling. Caching must be sharded across deterministic alphanumeric partitions to allow concurrent, parallel querying.

---

## 2. Engineering Tradeoffs & Architectural Strategy

### A. Batch Windowing & Backpressure Management
Configured in `template.yaml`:
```yaml
Events:
  SourceTableStream:
    Type: DynamoDB
    Properties:
      Stream: !GetAtt SourceStreamArnLookup.StreamArn
      StartingPosition: TRIM_HORIZON
      BatchSize: 100
      MaximumBatchingWindowInSeconds: 10
      MaximumRetryAttempts: 3
      BisectBatchOnFunctionError: true
      DestinationConfig:
        OnFailure:
          Destination: !GetAtt StreamProcessingDLQ.Arn
      FunctionResponseTypes:
        - ReportBatchItemFailures
```
- **10-Second Batch Window**: Accumulates records for up to 10 seconds before triggering invocation, smoothing bursts and cutting Lambda invocations by over 70%.
- **Partial Batch Failures (`ReportBatchItemFailures`)**: When a specific record fails deserialization, `load_consumer_service.py` returns its `sequenceNumber` in `batchItemFailures`, ensuring AWS only retries that specific record.
- **Poison-Pill Isolation via SQS DLQ**: If a record exhausts 3 retries, AWS automatically routes it to `StreamProcessingDLQ`, unblocking the stream shard and triggering `StreamProcessingDLQAlarm`.

### B. 36-Partition Sharded Caching Scheme
Enriched records are transformed and written into `cache.report` with a deterministic partition key:
- **`PK`**: `job-x#{last_char_of_id}` (e.g. `job-x#A`, `job-x#1`) — distributes data uniformly across 36 alphanumeric partitions (A–Z, 0–9).
- **`SK`**: `{date_string}#{id}` (e.g. `20261029#CUSIP9981A`).

### C. Concurrent Multi-Partition Query Fanout (`report_service.py`)
- When triggered by `job-service` on `load-end`, the report generator initiates a concurrent query fanout across all 36 partitions using a `ThreadPoolExecutor(max_workers=10)`:
  - Each thread executes a `begins_with(YYYYMMDD)` range query on its assigned partition key.
  - Aggregates records in memory, serializes to UTF-8 CSV via `csv.DictWriter`, and streams to S3 (`archive/job-v1/report_{date}.csv`).
  - Resolves email distribution lists from AWS SSM Parameter Store and delivers the CSV as a raw MIME attachment via Amazon Pinpoint.
  - Externalizes the DynamoDB Table resource instantiation into a cached singleton (`get_report_table()`) at module level to eliminate per-worker compute overhead.

---

## 3. Operational Constraints & SLA Targets

| Operational Parameter | Production Target | Design Justification |
|---|---|---|
| **Stream Batch Window** | `10 seconds` | Buffers high-frequency CDC events, maximizing 25-item DynamoDB batch write efficiency. |
| **Max Retry Ceiling** | `3 attempts` | Limits retry duration before routing poison pills to SQS DLQ. |
| **Report Query Fanout** | 10 concurrent threads | Completes 36-partition aggregation within 45 seconds without throttling table read capacity. |
| **IAM Least Privilege** | 100% exact ARN matches | Zero wildcard (`*`) table, bucket, or Pinpoint application permissions. |

---

## 4. Repository Structure

```
stream-processing-pipeline/
├── src/
│   ├── load_consumer_service.py   # Stream consumer with batchItemFailures & End_Of_File listener
│   ├── report_service.py          # 36-partition parallel query aggregator & Pinpoint mailer
│   ├── requirements.txt           # Runtime dependencies
│   └── utils.py                   # Boto3 client getters, DynamoDB paginator, MIME email builder
├── tests/
│   ├── __init__.py                # Test setup & SDK mocks
│   ├── test_load_consumer_service.py  # Tests for batchItemFailures, eof markers, stream records
│   ├── test_report_service.py     # Tests for 36-partition fanout, CSV building, S3 upload
│   └── test_utils.py              # Tests for DynamoDB query pagination and Pinpoint dispatches
├── template.yaml                  # SAM infrastructure (Stream EventSourceMapping, SQS DLQ, Alarms)
├── build.yaml                     # AWS CodeBuild CI/CD specification
├── diagram.md                     # Sequence and ER diagrams
└── readme.md                      # Subsystem documentation
```

---

## 5. Local Verification & Unit Testing

```bash
python -m unittest discover tests -v
```
