"""
empty_bucket.py – CloudFormation Custom Resource: EmptyBucket using crhelper.

Empties all versions, delete markers, and unversioned objects in an S3 bucket prior
to CloudFormation stack deletion. Solves the standard CloudFormation teardown error:
"The bucket you are attempting to delete is not empty".
"""

from __future__ import annotations

from typing import Any, Dict

from crhelper import CfnResource
from botocore.exceptions import BotoCoreError, ClientError

from utils import (
    get_s3_resource,
    logger,
    tracer,
)

helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")


def purge_s3_bucket(bucket_name: str) -> None:
    """
    Purge all object versions, delete markers, and unversioned objects from an S3 bucket.
    """
    s3_resource = get_s3_resource()
    bucket = s3_resource.Bucket(bucket_name)

    logger.info("Purging object versions and delete markers", extra={"bucket_name": bucket_name})
    bucket.object_versions.delete()

    logger.info("Purging any remaining unversioned objects", extra={"bucket_name": bucket_name})
    bucket.objects.all().delete()


@helper.create
@helper.update
def create_or_update(event: Dict[str, Any], context: Any) -> str:
    """No-op on create/update; returns physical resource ID."""
    props = event.get("ResourceProperties", {})
    bucket_name = props.get("BucketName", "")
    return f"empty-bucket-{bucket_name or 'unknown'}"


@helper.delete
def delete(event: Dict[str, Any], context: Any) -> str:
    """Purge bucket contents prior to bucket deletion."""
    props = event.get("ResourceProperties", {})
    bucket_name = props.get("BucketName", "")
    physical_id = f"empty-bucket-{bucket_name or 'unknown'}"

    if not bucket_name:
        logger.warning("No BucketName provided in ResourceProperties; skipping purge")
        return physical_id

    try:
        logger.info("Starting bucket purge before deletion", extra={"bucket_name": bucket_name})
        purge_s3_bucket(bucket_name)
        logger.info("Successfully emptied bucket", extra={"bucket_name": bucket_name})
    except Exception as exc:
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if error_code in ("NoSuchBucket", "404") or "NoSuchBucket" in str(exc):
            logger.info("Bucket already does not exist; marking deletion as SUCCESS", extra={"bucket_name": bucket_name})
        else:
            logger.exception("Failed to empty bucket", extra={"bucket_name": bucket_name})
            raise

    return physical_id


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """Handle EmptyBucket Custom Resource lifecycle via crhelper."""
    helper(event, context)


# Backwards compatibility alias
handler = lambda_handler
