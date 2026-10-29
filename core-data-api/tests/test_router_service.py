"""Unit tests for router_service.py response helpers and route utilities."""

import os
import sys
import unittest
from unittest.mock import MagicMock

# Mock aws_lambda_powertools before import
try:
    import aws_lambda_powertools
except ImportError:
    mock_powertools = MagicMock()
    mock_logger = MagicMock()
    mock_powertools.Logger = MagicMock(return_value=mock_logger)
    sys.modules["aws_lambda_powertools"] = mock_powertools

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from router_service import (
    build_route_table,
    error_response,
    resolve_route,
    success_response,
)


def dummy_handler(event, params):
    return success_response({"msg": "ok", "params": params})


class TestRouterService(unittest.TestCase):
    def test_build_route_table_and_resolve(self):
        routes = [
            ("POST", "/v1/account/inquiry", dummy_handler, "account-inquiry"),
            ("GET", "/v1/activity/{account_id}", dummy_handler, "activity-lookup"),
        ]
        table = build_route_table(routes)
        self.assertEqual(len(table), 2)

        # Match POST
        res = resolve_route(table, "POST", "/v1/account/inquiry")
        self.assertIsNotNone(res)
        handler, params, name = res
        self.assertEqual(name, "account-inquiry")
        self.assertEqual(params, {})

        # Match GET with param
        res_get = resolve_route(table, "GET", "/v1/activity/ACC123")
        self.assertIsNotNone(res_get)
        _, params_get, name_get = res_get
        self.assertEqual(name_get, "activity-lookup")
        self.assertEqual(params_get, {"account_id": "ACC123"})

        # No match
        self.assertIsNone(resolve_route(table, "GET", "/v1/unknown"))
        self.assertIsNone(resolve_route(table, "DELETE", "/v1/account/inquiry"))

    def test_response_helpers(self):
        succ = success_response({"status": "healthy"}, status_code=200)
        self.assertEqual(succ["statusCode"], 200)
        self.assertIn("healthy", succ["body"])
        self.assertEqual(succ["headers"]["Content-Type"], "application/json")

        err = error_response("Not found", status_code=404, error_code="NOT_FOUND")
        self.assertEqual(err["statusCode"], 404)
        self.assertIn("Not found", err["body"])
        self.assertIn("NOT_FOUND", err["body"])


if __name__ == "__main__":
    unittest.main()
