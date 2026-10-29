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
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "test-shared-utils")
os.environ.setdefault("POWERTOOLS_LOG_LEVEL", "DEBUG")
os.environ.setdefault("ENVIRONMENT", "test")


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

# Mock crhelper
class MockCfnResource:
    def __init__(self, *args, **kwargs):
        self.Data: Dict[str, Any] = {}
        self._create_fn = None
        self._update_fn = None
        self._delete_fn = None

    def create(self, f):
        self._create_fn = f
        return f

    def update(self, f):
        self._update_fn = f
        return f

    def delete(self, f):
        self._delete_fn = f
        return f

    def __call__(self, event, context):
        req_type = event.get("RequestType", "Create")
        if req_type == "Create" and self._create_fn:
            return self._create_fn(event, context)
        elif req_type == "Update" and self._update_fn:
            return self._update_fn(event, context)
        elif req_type == "Delete" and self._delete_fn:
            return self._delete_fn(event, context)


if "crhelper" not in sys.modules:
    mock_cr = MagicMock()
    mock_cr.CfnResource = MockCfnResource
    sys.modules["crhelper"] = mock_cr

# Mock boto3 if not installed
if "boto3" not in sys.modules:
    sys.modules["boto3"] = MagicMock()
