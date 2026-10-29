"""Mock AWS dependencies (boto3, botocore, powertools) for local test execution."""

import os
import sys
from typing import Any, Dict
from unittest.mock import MagicMock

# Ensure repo and src directories are in sys.path
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_DIR, "src")
for p in (REPO_DIR, SRC_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

# Set test environment variables
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "test-stream-pipeline")
os.environ.setdefault("POWERTOOLS_LOG_LEVEL", "DEBUG")
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("REFERENCE_TABLE_NAME", "test-reference-table")
os.environ.setdefault("CACHE_REPORT_TABLE_NAME", "test-cache-report")
os.environ.setdefault("JOBS_TABLE_NAME", "test-jobs")
os.environ.setdefault("ARCHIVE_BUCKET_NAME", "test-archive-bucket")
os.environ.setdefault("EMAIL_SSM_PARAM_NAME", "/test/config/report-emails")
os.environ.setdefault("PINPOINT_APP_ID", "test-pinpoint-app")
os.environ.setdefault("PINPOINT_FROM_EMAIL", "reports@example.com")
os.environ.setdefault("JOB_PREFIX", "job-x")
os.environ.setdefault("AWS_LAMBDA_FUNCTION_NAME", "load")


# Mock botocore exceptions
class BotoCoreError(Exception):
    pass


class ClientError(BotoCoreError):
    def __init__(self, error_response: Dict[str, Any], operation_name: str):
        super().__init__(str(error_response))
        self.response = error_response
        self.operation_name = operation_name


if "botocore" not in sys.modules:
    mock_botocore = MagicMock()
    mock_botocore_exceptions = MagicMock()
    mock_botocore_exceptions.BotoCoreError = BotoCoreError
    mock_botocore_exceptions.ClientError = ClientError
    mock_botocore.exceptions = mock_botocore_exceptions
    mock_botocore_config = MagicMock()
    mock_botocore.config = mock_botocore_config
    sys.modules["botocore"] = mock_botocore
    sys.modules["botocore.exceptions"] = mock_botocore_exceptions
    sys.modules["botocore.config"] = mock_botocore_config
else:
    if not hasattr(sys.modules["botocore"], "exceptions"):
        mock_botocore_exceptions = MagicMock()
        mock_botocore_exceptions.BotoCoreError = BotoCoreError
        mock_botocore_exceptions.ClientError = ClientError
        sys.modules["botocore.exceptions"] = mock_botocore_exceptions


def passthrough_decorator(*args, **kwargs):
    if len(args) == 1 and callable(args[0]) and not kwargs:
        return args[0]
    def decorator(f):
        return f
    return decorator


# Mock aws_lambda_powertools
if "aws_lambda_powertools" not in sys.modules:
    mock_powertools = MagicMock()
    mock_logger = MagicMock()
    mock_logger.inject_lambda_context = passthrough_decorator
    mock_tracer = MagicMock()
    mock_tracer.capture_lambda_handler = passthrough_decorator
    mock_tracer.capture_method = passthrough_decorator

    mock_powertools.Logger = MagicMock(return_value=mock_logger)
    mock_powertools.Tracer = MagicMock(return_value=mock_tracer)
    sys.modules["aws_lambda_powertools"] = mock_powertools

# Mock boto3 and dynamodb condition/types
class MockCondition:
    def __init__(self, name: str):
        self.name = name

    def eq(self, val):
        return self

    def begins_with(self, val):
        return self

    def __and__(self, other):
        return self


class MockKey:
    def __init__(self, name: str):
        self.name = name

    def eq(self, val):
        return MockCondition(self.name)

    def begins_with(self, val):
        return MockCondition(self.name)


class MockTypeDeserializer:
    def deserialize(self, value):
        if not isinstance(value, dict):
            return value
        if "S" in value:
            return value["S"]
        if "N" in value:
            n = value["N"]
            return int(n) if "." not in n else float(n)
        if "BOOL" in value:
            return value["BOOL"]
        if "M" in value:
            return {k: self.deserialize(v) for k, v in value["M"].items()}
        if "L" in value:
            return [self.deserialize(v) for v in value["L"]]
        raise TypeError(f"Unsupported DynamoDB type: {value}")


if "boto3" not in sys.modules:
    mock_boto3 = MagicMock()
    sys.modules["boto3"] = mock_boto3

mock_conditions = MagicMock()
mock_conditions.Key = MockKey
sys.modules["boto3.dynamodb.conditions"] = mock_conditions

mock_types = MagicMock()
mock_types.TypeDeserializer = MockTypeDeserializer
sys.modules["boto3.dynamodb.types"] = mock_types
