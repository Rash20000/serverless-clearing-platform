"""Mock AWS dependencies (boto3, botocore, powertools, urllib3) for local test execution."""

import json
import os
import sys
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

# Ensure repo and src directories are in sys.path
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_DIR, "src")
for p in (REPO_DIR, SRC_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

# Set test environment variables
os.environ.setdefault("POWERTOOLS_SERVICE_NAME", "test-partner-egress")
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
    mock_botocore.__path__ = []
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


# Mock event and resolver
class MockEvent:
    def __init__(self, json_body=None):
        self.json_body = json_body or {}


class BadRequestError(Exception):
    pass


class NotFoundError(Exception):
    pass


class InternalServerError(Exception):
    pass


class MockAPIGatewayRestResolver:
    def __init__(self, *args, **kwargs):
        self._routes = {}
        self.current_event = MockEvent()

    def get(self, path):
        def decorator(f):
            self._routes[("GET", path)] = f
            return f
        return decorator

    def post(self, path):
        def decorator(f):
            self._routes[("POST", path)] = f
            return f
        return decorator

    def resolve(self, event, context):
        path = event.get("path", "")
        method = event.get("httpMethod", "GET")
        body = event.get("body")
        if isinstance(body, str):
            try:
                self.current_event = MockEvent(json.loads(body))
            except Exception:
                self.current_event = MockEvent({})
        elif isinstance(body, dict):
            self.current_event = MockEvent(body)
        else:
            self.current_event = MockEvent({})

        for (m, p), fn in self._routes.items():
            if m == method:
                if "<" in p:
                    prefix = p.split("<")[0]
                    if path.startswith(prefix):
                        param = path[len(prefix):].split("/")[0]
                        try:
                            data = fn(param)
                            return {"statusCode": 200, "body": json.dumps(data) if isinstance(data, dict) else data}
                        except BadRequestError as e:
                            return {"statusCode": 400, "body": str(e)}
                        except Exception as e:
                            return {"statusCode": 500, "body": str(e)}
                elif path == p:
                    try:
                        data = fn()
                        return {"statusCode": 200, "body": json.dumps(data) if isinstance(data, dict) else data}
                    except BadRequestError as e:
                        return {"statusCode": 400, "body": str(e)}
                    except Exception as e:
                        return {"statusCode": 500, "body": str(e)}

        return {"statusCode": 404, "body": "Not Found"}


# Mock aws_lambda_powertools
if "aws_lambda_powertools" not in sys.modules:
    mock_powertools = MagicMock()
    mock_powertools.__path__ = []
    mock_logger = MagicMock()
    mock_logger.inject_lambda_context = passthrough_decorator
    mock_tracer = MagicMock()
    mock_tracer.capture_lambda_handler = passthrough_decorator
    mock_tracer.capture_method = passthrough_decorator

    mock_metrics = MagicMock()
    mock_metrics.log_metrics = passthrough_decorator

    mock_powertools.Logger = MagicMock(return_value=mock_logger)
    mock_powertools.Tracer = MagicMock(return_value=mock_tracer)
    mock_powertools.Metrics = MagicMock(return_value=mock_metrics)

    sys.modules["aws_lambda_powertools"] = mock_powertools
    sys.modules["aws_lambda_powertools.metrics"] = MagicMock()
    sys.modules["aws_lambda_powertools.utilities.typing"] = MagicMock()

mock_event_handler = MagicMock()
mock_event_handler.__path__ = []
mock_event_handler.APIGatewayRestResolver = MockAPIGatewayRestResolver
mock_event_handler.Response = MagicMock()
mock_event_exceptions = MagicMock()
mock_event_exceptions.__path__ = []
mock_event_exceptions.BadRequestError = BadRequestError
mock_event_exceptions.NotFoundError = NotFoundError
mock_event_exceptions.InternalServerError = InternalServerError
mock_event_handler.exceptions = mock_event_exceptions
sys.modules["aws_lambda_powertools.event_handler"] = mock_event_handler
sys.modules["aws_lambda_powertools.event_handler.exceptions"] = mock_event_exceptions

# Mock boto3 if not installed
if "boto3" not in sys.modules:
    mock_boto3 = MagicMock()
    mock_boto3.__path__ = []
    sys.modules["boto3"] = mock_boto3

# Mock urllib3 if not installed
if "urllib3" not in sys.modules:
    mock_urllib3 = MagicMock()
    mock_urllib3.__path__ = []
    mock_pool = MagicMock()
    mock_urllib3.PoolManager = MagicMock(return_value=mock_pool)
    mock_urllib3.Retry = MagicMock()
    mock_urllib3.Timeout = MagicMock()
    mock_urllib3.util = MagicMock()
    sys.modules["urllib3"] = mock_urllib3
    sys.modules["urllib3.util"] = mock_urllib3.util
