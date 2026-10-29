"""
Report Service — Lambda that queries cache.report, builds CSV export, archives to S3,
and delivers the report via AWS Pinpoint using email IDs retrieved from AWS SSM Parameter Store.

Has NO direct CloudFormation event trigger.
Invoked by the Job Monitor when `load-end` is detected in the jobs DynamoDB table.
Upon completion, inserts `report-end` into the DynamoDB jobs table.

Environment variables:
    CACHE_REPORT_TABLE_NAME : DynamoDB cache.report table (imported from shared infra)
    ARCHIVE_BUCKET_NAME     : S3 archive bucket (imported from shared S3 infra)
    JOBS_TABLE_NAME         : DynamoDB jobs table for tracking `report-end` (imported from shared job monitor stack)
    EMAIL_SSM_PARAM_NAME    : AWS SSM Parameter Store path containing plain-text recipient emails (e.g. finance ops, trading floor)
    PINPOINT_APP_ID         : AWS Pinpoint Application / Project ID (shared pinpoint)
    PINPOINT_FROM_EMAIL     : Verified sender email address in Pinpoint
    JOB_PREFIX              : Partition key prefix (default: job-x)
"""

from __future__ import annotations

import csv
import io
import os
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from utils import (
    get_current_date_string,
    get_dynamodb_resource,
    get_s3_client,
    get_ssm_parameter,
    logger,
    query_all_items,
    record_job_end,
    send_email_via_pinpoint,
    tracer,
)


@tracer.capture_method
def get_recipient_emails_from_ssm(param_name: str) -> list[str]:
    """
    Fetch and parse recipient email IDs from SSM Parameter Store (plain text comma-separated or newline-separated).
    E.g. finance ops, trading floor distribution lists.
    """
    raw_val = get_ssm_parameter(param_name)
    emails: list[str] = []
    for line in raw_val.splitlines():
        for item in line.split(","):
            clean = item.strip()
            if clean:
                emails.append(clean)
    return emails

CACHE_REPORT_TABLE_NAME: str = os.environ.get("CACHE_REPORT_TABLE_NAME", "")
ARCHIVE_BUCKET_NAME: str = os.environ.get("ARCHIVE_BUCKET_NAME", "")
JOBS_TABLE_NAME: str = os.environ.get("JOBS_TABLE_NAME", "")
EMAIL_SSM_PARAM_NAME: str = os.environ.get("EMAIL_SSM_PARAM_NAME", "")
PINPOINT_APP_ID: str = os.environ.get("PINPOINT_APP_ID", "")
PINPOINT_FROM_EMAIL: str = os.environ.get("PINPOINT_FROM_EMAIL", "reports@example.com")
JOB_PREFIX: str = os.environ.get("JOB_PREFIX", "job-x")

CSV_FIELDS: list[str] = [
    "id",
    "status",
    "amount",
    "processed_at",
    "reference_id",
    "reference_type",
    "reference_status",
]

_PARTITION_SUFFIXES: list[str] = list(string.ascii_uppercase) + list(string.digits)

# Cached module-level table resource reference
_cache_report_table: Any = None


def get_report_table() -> Any:
    """Return cached DynamoDB Table resource instance for cache.report."""
    global _cache_report_table
    if _cache_report_table is None:
        _cache_report_table = get_dynamodb_resource().Table(CACHE_REPORT_TABLE_NAME)
    return _cache_report_table


# ---------------------------------------------------------------------------
# Data Query
# ---------------------------------------------------------------------------

def _query_partition(table: Any, partition_key: str, date_string: str) -> list[dict]:
    """Query a single partition key for items whose SK begins with date_string."""
    key_condition = Key("PK").eq(partition_key) & Key("SK").begins_with(date_string)
    return query_all_items(table=table, key_condition_expression=key_condition)


@tracer.capture_method
def fetch_report_data(date_string: str) -> list[dict]:
    """Query all 36 alphanumeric partitions concurrently via ThreadPoolExecutor."""
    partition_keys = [f"{JOB_PREFIX}#{suffix}" for suffix in _PARTITION_SUFFIXES]
    aggregated: list[dict] = []
    table = get_report_table()

    with ThreadPoolExecutor(max_workers=min(len(partition_keys), 10)) as executor:
        future_to_pk = {
            executor.submit(_query_partition, table, pk, date_string): pk
            for pk in partition_keys
        }
        for future in as_completed(future_to_pk):
            pk = future_to_pk[future]
            try:
                aggregated.extend(future.result())
            except Exception:
                logger.exception("Failed to query partition", extra={"partition_key": pk})

    logger.info("Report query completed", extra={"date_string": date_string, "total_records": len(aggregated)})
    return aggregated


# ---------------------------------------------------------------------------
# CSV & S3
# ---------------------------------------------------------------------------

def build_csv(records: list[dict]) -> bytes:
    """Build UTF-8 CSV bytes from record dictionaries."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=CSV_FIELDS,
        restval="",
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(records)
    return buffer.getvalue().encode("utf-8")


def upload_to_s3(csv_bytes: bytes, date_string: str) -> str:
    """Archive CSV report into shared S3 bucket under archive/job-v1/."""
    s3_key = f"archive/job-v1/report_{date_string}.csv"
    get_s3_client().put_object(
        Bucket=ARCHIVE_BUCKET_NAME,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info("Report uploaded to shared S3", extra={"bucket": ARCHIVE_BUCKET_NAME, "key": s3_key})
    return s3_key


# ---------------------------------------------------------------------------
# Email Dispatch via AWS Pinpoint
# ---------------------------------------------------------------------------

@tracer.capture_method
def deliver_report_email(csv_bytes: bytes, date_string: str, record_count: int, s3_key: str) -> dict:
    """
    Fetch email recipients from SSM Parameter Store (plain text for finance ops, trading floor)
    and dispatch email with CSV attachment via shared AWS Pinpoint.
    """
    if not EMAIL_SSM_PARAM_NAME or not PINPOINT_APP_ID:
        logger.warning(
            "EMAIL_SSM_PARAM_NAME or PINPOINT_APP_ID not configured; skipping email delivery",
            extra={"ssm_param": EMAIL_SSM_PARAM_NAME, "pinpoint_app": PINPOINT_APP_ID},
        )
        return {"status": "SKIPPED", "reason": "Missing email/Pinpoint configuration"}

    recipients = get_recipient_emails_from_ssm(EMAIL_SSM_PARAM_NAME)
    if not recipients:
        logger.warning("No email recipients found in SSM", extra={"param": EMAIL_SSM_PARAM_NAME})
        return {"status": "SKIPPED", "reason": "No recipients found"}

    filename = f"report_{date_string}.csv"
    subject = f"Daily Processing Report - {date_string}"
    html_body = f"""
    <html>
      <body>
        <h2>Daily Processing Report</h2>
        <p>The daily data processing pipeline has completed successfully.</p>
        <ul>
          <li><strong>Date:</strong> {date_string}</li>
          <li><strong>Total Records Processed:</strong> {record_count}</li>
          <li><strong>S3 Archive Path:</strong> s3://{ARCHIVE_BUCKET_NAME}/{s3_key}</li>
        </ul>
        <p>Attached is the complete CSV report export.</p>
      </body>
    </html>
    """

    resp = send_email_via_pinpoint(
        application_id=PINPOINT_APP_ID,
        from_address=PINPOINT_FROM_EMAIL,
        to_addresses=recipients,
        subject=subject,
        html_body=html_body,
        attachment_bytes=csv_bytes,
        attachment_filename=filename,
    )
    return resp


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Report Lambda handler (invoked on-demand by external Job Monitor).
    1. Queries cache.report across partitions for target date
    2. Builds CSV report
    3. Archives to shared S3 (archive/job-v1/report_{date}.csv)
    4. Retrieves distribution list from AWS SSM Parameter Store & sends via AWS Pinpoint
    5. Inserts `report-end` into DynamoDB jobs table
    """
    date_string: str = event.get("date") or get_current_date_string()
    logger.info("Report job execution started", extra={"date_string": date_string, "event": event})

    try:
        # 1. Fetch data
        records = fetch_report_data(date_string)

        # 2. Build CSV
        csv_bytes = build_csv(records)

        # 3. Upload to shared S3
        s3_key = upload_to_s3(csv_bytes, date_string)

        # 4. Deliver email via Pinpoint using SSM Parameter Store recipients
        email_result = deliver_report_email(csv_bytes, date_string, len(records), s3_key)

        # 5. Record job completion in DynamoDB jobs table
        record_job_end(
            jobs_table_name=JOBS_TABLE_NAME,
            function_name="report",
            status="COMPLETED",
            metadata={
                "date": date_string,
                "s3_key": s3_key,
                "record_count": len(records),
                "email_status": email_result.get("status", "SENT"),
            },
        )

        return {
            "statusCode": 200,
            "message": "Report generated and delivered successfully",
            "date": date_string,
            "s3_key": s3_key,
            "record_count": len(records),
        }

    except Exception as exc:
        logger.exception("Report job failed", extra={"date_string": date_string})
        # Record failure status
        record_job_end(
            jobs_table_name=JOBS_TABLE_NAME,
            function_name="report",
            status="FAILED",
            metadata={"date": date_string, "error": str(exc)},
        )
        raise


# Backwards compatibility alias
handler = lambda_handler

