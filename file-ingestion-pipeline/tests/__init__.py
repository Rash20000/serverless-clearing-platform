"""Mock AWS dependencies (boto3, botocore, powertools) for local test execution."""

import json
import os
import sys
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

# Ensure src is in sys.path
SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Set test environment variables
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "test-file-ingestion")
os.environ.setdefault("POWERTOOLS_LOG_LEVEL", "DEBUG")
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("TARGET_TABLE_NAME", "test-raw-records")
os.environ.setdefault("LOAD_BUCKET", "test-landing-bucket")
os.environ.setdefault("LOAD_KEY", "inbound/feeds/test_data.dat")
os.environ.setdefault("ROWS_PER_CHUNK", "100")
os.environ.setdefault("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123456789012:stateMachine:test-pipeline")


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
    sys.modules["botocore"] = mock_botocore
    sys.modules["botocore.exceptions"] = mock_botocore_exceptions
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

# Mock boto3 if not installed
if "boto3" not in sys.modules:
    sys.modules["boto3"] = MagicMock()
    sys.modules["boto3.dynamodb.conditions"] = MagicMock()

