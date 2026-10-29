"""Unit tests for account_service.py."""

import json
import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import account_service


class TestAccountService(unittest.TestCase):
    def setUp(self):
        self.context = MagicMock()
        self.context.aws_request_id = "req-123"

    def test_options_preflight(self):
        event = {"httpMethod": "OPTIONS", "body": None}
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)

    def test_warmup_event(self):
        event = {"action": "warmup", "source": "aws.events"}
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["status"], "warm")

    @patch("account_service.put_event")
    @patch("account_service.query_cache_table")
    def test_account_inquiry_cache_hit(self, mock_query, mock_put_event):
        mock_query.return_value = [{"accountId": "ACC100", "balance": 1500, "status": "active"}]
        mock_put_event.return_value = {"FailedEntryCount": 0}

        event = {
            "body": json.dumps({"account_id": "ACC100", "fetch_activity": False}),
            "httpMethod": "POST",
            "path": "/v1/account/inquiry",
        }
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)

        body = json.loads(response["body"])
        self.assertEqual(body["accountId"], "ACC100")
        self.assertTrue(body["cached"])
        self.assertEqual(body["account"]["balance"], 1500)
        self.assertIsNone(body["activity"])

        mock_put_event.assert_called_once_with(
            source="inbound.api",
            detail_type="QuoteCountIncremented",
            detail={"accountId": "ACC100"},
        )

    @patch("account_service.put_event")
    @patch("account_service.parallel_cache_queries")
    def test_account_inquiry_with_activity(self, mock_parallel, mock_put_event):
        mock_parallel.return_value = (
            [{"accountId": "ACC200", "balance": 2500}],
            [{"accountId": "ACC200", "activityId": "ACT1", "amount": 100}],
        )

        event = {
            "body": json.dumps({"account_id": "ACC200", "fetch_activity": True}),
            "httpMethod": "POST",
            "path": "/v1/account/inquiry",
        }
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)

        body = json.loads(response["body"])
        self.assertEqual(body["accountId"], "ACC200")
        self.assertTrue(body["cached"])
        self.assertEqual(len(body["activity"]), 1)

    @patch("account_service.call_outbound_gateway")
    @patch("account_service.put_event")
    @patch("account_service.query_cache_table")
    def test_account_inquiry_cache_miss_calls_outbound(self, mock_query, mock_put_event, mock_outbound):
        mock_query.return_value = []
        mock_outbound.return_value = {
            "status": 200,
            "body": json.dumps({"accountId": "ACC999", "status": "active"}),
        }

        event = {
            "body": json.dumps({"account_id": "ACC999", "fetch_activity": False}),
            "httpMethod": "POST",
            "path": "/v1/account/inquiry",
        }
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)

        body = json.loads(response["body"])
        self.assertEqual(body["accountId"], "ACC999")
        self.assertFalse(body["cached"])
        self.assertEqual(body["account"]["status"], "active")

        mock_outbound.assert_called_once_with(
            path="/v1/account/lookup",
            payload={"account_id": "ACC999"},
        )

    def test_account_inquiry_missing_body(self):
        event = {"body": None, "httpMethod": "POST"}
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 400)

    def test_account_inquiry_missing_account_id(self):
        event = {"body": json.dumps({"fetch_activity": True}), "httpMethod": "POST"}
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 400)
        self.assertIn("account_id is required", response["body"])

    @patch("account_service.query_cache_table")
    def test_account_inquiry_database_error(self, mock_query):
        mock_query.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}},
            "Query",
        )
        event = {
            "body": json.dumps({"account_id": "ACCERR"}),
            "httpMethod": "POST",
        }
        response = account_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 502)


if __name__ == "__main__":
    unittest.main()
