"""
job_watcher.py – Job Watcher Service for the central Jobs catalog.

Responsibilities:
    1. Listens to DynamoDB Streams from the Jobs table (${Environment}-jobs).
    2. Detects milestone markers with PK == 'load-end'.
    3. If status == 'COMPLETED':
       - Queries REPORT_REGISTRY in Jobs table for registered report jobs.
       - Dispatches asynchronous Lambda invocations to kick off downstream report generation.
    4. If status == 'FAILED':
       - Dispatches an urgent failure alert to support via Amazon Pinpoint
         with subject "ALERT: Job Failed ({date}) - Review error and retry if safe".
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from boto3.dynamodb.types import TypeDeserializer
from utils import (
    get_env,
    invoke_lambda_async,
    logger,
    lookup_registered_report_job,
    send_pinpoint_email,
    tracer,
)

_deserializer = TypeDeserializer()

JOBS_TABLE_NAME: str = os.environ.get("JOBS_TABLE_NAME", "")
PINPOINT_APP_ID: str = os.environ.get("PINPOINT_APP_ID", "")
PINPOINT_FROM_EMAIL: str = os.environ.get("PINPOINT_FROM_EMAIL", "support-alerts@example.com")
SUPPORT_EMAIL: str = os.environ.get("SUPPORT_EMAIL", "support-team@example.com")
DEFAULT_REPORT_JOB_ID: str = os.environ.get("DEFAULT_REPORT_JOB_ID", "daily-summary-report")

_ACCEPTED_EVENT_NAMES = frozenset({"INSERT", "MODIFY"})
_TRIGGER_PK = "load-end"


def build_failure_alert_html(job_item: Dict[str, Any]) -> str:
    """Construct HTML body for Pinpoint failure alert notification."""
    date_str = str(job_item.get("date", "Unknown"))
    error_details = str(
        job_item.get("error")
        or job_item.get("metadata", {}).get("error")
        or "No specific error trace provided"
    )
    pk_val = str(job_item.get("PK", ""))
    sk_val = str(job_item.get("SK", ""))
    completed_at = str(job_item.get("completed_at", "N/A"))

    return f"""<!DOCTYPE html>
<html>
  <head>
    <style>
      body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; color: #333; }}
      h2 {{ color: #D32F2F; }}
      table {{ border-collapse: collapse; width: 100%; max-width: 600px; }}
      td {{ padding: 8px 12px; border: 1px solid #e0e0e0; }}
      td.header {{ background-color: #f7f7f7; font-weight: bold; width: 150px; }}
      pre {{ background-color: #fce4ec; color: #c2185b; padding: 10px; border-radius: 4px; overflow-x: auto; }}
    </style>
  </head>
  <body>
    <h2>Job Failure Notification</h2>
    <p>A data ingestion or processing job has reported a <strong>FAILED</strong> status.</p>
    <table>
      <tr><td class="header">Job PK</td><td>{pk_val}</td></tr>
      <tr><td class="header">Job SK</td><td>{sk_val}</td></tr>
      <tr><td class="header">Date</td><td>{date_str}</td></tr>
      <tr><td class="header">Completed At</td><td>{completed_at}</td></tr>
      <tr><td class="header">Error Details</td><td><pre>{error_details}</pre></td></tr>
    </table>
    <br/>
    <p><strong>Action Required:</strong> Review the error trace in CloudWatch logs and execute retry if safe.</p>
  </body>
</html>"""


def process_stream_record(item: Dict[str, Any]) -> str:
    """
    Evaluate a single deserialized stream item.
    Returns: 'SUCCESS_TRIGGERED', 'FAILURE_ALERTED', or 'IGNORED'.
    """
    pk = item.get("PK", "")
    status = item.get("status", "")
    date_str = item.get("date", "")

    if pk != _TRIGGER_PK:
        return "IGNORED"

    # Branch 1: Job Failure -> Alert Support
    if status == "FAILED":
        logger.error(
            "Detected 'load-end' with FAILED status; preparing support alert",
            extra={"job_item": item},
        )
        subject = f"ALERT: Job Failed ({date_str or 'Unknown'}) - Review error and retry if safe"
        html_body = build_failure_alert_html(item)
        send_pinpoint_email(
            app_id=PINPOINT_APP_ID,
            from_email=PINPOINT_FROM_EMAIL,
            to_email=SUPPORT_EMAIL,
            subject=subject,
            html_body=html_body,
        )
        return "FAILURE_ALERTED"

    # Branch 2: Job Completed -> Trigger Registered Downstream Report
    if status == "COMPLETED":
        logger.info(
            "Detected 'load-end' with COMPLETED status; checking report registry",
            extra={"job_item": item},
        )
        if not JOBS_TABLE_NAME:
            logger.warning("JOBS_TABLE_NAME is not configured; cannot lookup report registry")
            return "IGNORED"

        registry_item = lookup_registered_report_job(JOBS_TABLE_NAME, DEFAULT_REPORT_JOB_ID)
        if registry_item and registry_item.get("enabled"):
            function_arn = registry_item.get("report_function_arn")
            if function_arn:
                logger.info(
                    "Triggering registered Report Lambda",
                    extra={"function_arn": function_arn, "date": date_str},
                )
                invoke_lambda_async(
                    function_name=function_arn,
                    payload={"date": date_str, "trigger": _TRIGGER_PK, "source": "job-watcher"},
                )
                return "SUCCESS_TRIGGERED"
            else:
                logger.warning("Registered report job missing report_function_arn", extra={"item": registry_item})
        else:
            logger.info("No active report job registered for trigger", extra={"job_id": DEFAULT_REPORT_JOB_ID})
        return "IGNORED"

    return "IGNORED"


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Entry-point for the Job Watcher Lambda.
    Processes DynamoDB Streams batches from ${Environment}-jobs.
    """
    records: List[Dict[str, Any]] = event.get("Records", [])
    logger.info("Job Watcher received stream batch", extra={"record_count": len(records)})

    invocations = 0
    failures_handled = 0

    for record in records:
        if record.get("eventName") not in _ACCEPTED_EVENT_NAMES:
            continue

        raw_new_image = record.get("dynamodb", {}).get("NewImage", {})
        if not raw_new_image:
            continue

        try:
            item = _deserializer.deserialize({"M": raw_new_image})
        except Exception:
            logger.exception("Failed to deserialize stream NewImage record")
            continue

        outcome = process_stream_record(item)
        if outcome == "SUCCESS_TRIGGERED":
            invocations += 1
        elif outcome == "FAILURE_ALERTED":
            failures_handled += 1

    logger.info(
        "Job Watcher batch complete",
        extra={"invocations": invocations, "failures_handled": failures_handled},
    )
    return {
        "statusCode": 200,
        "invocations": invocations,
        "failures_handled": failures_handled,
    }


# Backwards compatibility alias
handler = lambda_handler

