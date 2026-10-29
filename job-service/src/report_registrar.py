"""
report_registrar.py – Custom Resource for dynamic Report Job registration using crhelper.

Registers report job configurations, target function ARNs, and trigger keys ('load-end')
into the central Jobs metadata catalog table during CloudFormation stack creation/updates.
Removes registration on stack deletion.
"""

from __future__ import annotations

from typing import Any, Dict

from crhelper import CfnResource
from botocore.exceptions import BotoCoreError, ClientError

from utils import (
    get_dynamodb_resource,
    logger,
    tracer,
)

helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")


def register_job(
    table_name: str,
    job_id: str,
    report_function_arn: str,
    description: str,
    registered_at: str,
) -> Dict[str, Any]:
    """Insert or update report registration item in the Jobs catalog table."""
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    item = {
        "PK": "REPORT_REGISTRY",
        "SK": job_id,
        "job_id": job_id,
        "report_function_arn": report_function_arn,
        "description": description,
        "enabled": True,
        "registered_at": registered_at,
        "trigger_key": "load-end",
    }
    table.put_item(Item=item)
    return item


def unregister_job(table_name: str, job_id: str) -> None:
    """Delete report registration item from the Jobs catalog table."""
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    table.delete_item(Key={"PK": "REPORT_REGISTRY", "SK": job_id})


@helper.create
@helper.update
def create_or_update(event: Dict[str, Any], context: Any) -> str:
    """Register report job configuration in Jobs catalog table."""
    props = event.get("ResourceProperties", {})
    jobs_table_name = props.get("JobsTableName", "")
    job_id = props.get("JobId", "")
    report_function_arn = props.get("ReportFunctionArn", "")
    description = props.get("Description", "")
    registered_at = props.get("RegisteredAt", "CREATE_OR_UPDATE")

    physical_id = f"report-registration-{job_id}"

    if not jobs_table_name or not job_id:
        raise ValueError("Both 'JobsTableName' and 'JobId' are required in ResourceProperties")

    logger.info(
        "Registering report job with Jobs catalog",
        extra={"job_id": job_id, "jobs_table": jobs_table_name, "function_arn": report_function_arn},
    )
    register_job(
        table_name=jobs_table_name,
        job_id=job_id,
        report_function_arn=report_function_arn,
        description=description,
        registered_at=registered_at,
    )
    return physical_id


@helper.delete
def delete(event: Dict[str, Any], context: Any) -> str:
    """Unregister report job configuration from Jobs catalog table on stack delete."""
    props = event.get("ResourceProperties", {})
    jobs_table_name = props.get("JobsTableName", "")
    job_id = props.get("JobId", "")
    physical_id = f"report-registration-{job_id}"

    if jobs_table_name and job_id:
        try:
            logger.info("Unregistering report job on stack delete", extra={"job_id": job_id})
            unregister_job(jobs_table_name, job_id)
        except (ClientError, BotoCoreError) as err:
            logger.warning("Failed to unregister report on delete; proceeding cleanly", extra={"error": str(err)})

    return physical_id


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """Entry-point for ReportRegistrar Custom Resource via crhelper."""
    helper(event, context)


# Backwards compatibility alias
handler = lambda_handler
