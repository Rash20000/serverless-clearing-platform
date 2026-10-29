"""Unit tests for pages_service.py."""

import json
import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import pages_service


class TestPagesService(unittest.TestCase):
    def setUp(self):
        self.context = MagicMock()
        self.context.aws_request_id = "req-page-123"

    def test_options_preflight(self):
        event = {"httpMethod": "OPTIONS", "pathParameters": {}}
        response = pages_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)

    def test_warmup_event(self):
        event = {"action": "warmup", "source": "aws.events"}
        response = pages_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["status"], "warm")

    @patch("pages_service.query_cache_table")
    def test_pages_lookup_success(self, mock_query):
        mock_query.return_value = [{"accountId": "ACC100", "pageNumber": 1, "pageContent": "..."}]

        event = {
            "httpMethod": "GET",
            "path": "/v1/pages/ACC100",
            "pathParameters": {"account_id": "ACC100"},
        }
        response = pages_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["accountId"], "ACC100")
        self.assertEqual(len(body["pages"]), 1)

    def test_pages_lookup_missing_account_id(self):
        event = {
            "httpMethod": "GET",
            "path": "/v1/pages/",
            "pathParameters": {},
        }
        response = pages_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 400)

    @patch("pages_service.query_cache_table")
    def test_pages_lookup_database_error(self, mock_query):
        mock_query.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "DynamoDB unavailable"}},
            "Query",
        )
        event = {
            "httpMethod": "GET",
            "path": "/v1/pages/ACC100",
            "pathParameters": {"account_id": "ACC100"},
        }
        response = pages_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 502)


if __name__ == "__main__":
    unittest.main()
