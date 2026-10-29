"""Unit tests for activity_service.py."""

import json
import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import activity_service


class TestActivityService(unittest.TestCase):
    def setUp(self):
        self.context = MagicMock()
        self.context.aws_request_id = "req-act-123"

    def test_options_preflight(self):
        event = {"httpMethod": "OPTIONS", "pathParameters": {}}
        response = activity_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)

    def test_warmup_event(self):
        event = {"action": "warmup", "source": "aws.events"}
        response = activity_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["status"], "warm")

    @patch("activity_service.put_event")
    @patch("activity_service.query_cache_table")
    def test_activity_lookup_success(self, mock_query, mock_put_event):
        mock_query.return_value = [{"accountId": "ACC100", "activityId": "ACT-1"}]

        event = {
            "httpMethod": "GET",
            "path": "/v1/activity/ACC100",
            "pathParameters": {"account_id": "ACC100"},
        }
        response = activity_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["accountId"], "ACC100")
        self.assertEqual(len(body["activity"]), 1)

        mock_put_event.assert_called_once_with(
            source="inbound.api",
            detail_type="ActivityQueried",
            detail={"accountId": "ACC100", "resultCount": 1},
        )

    def test_activity_lookup_missing_account_id(self):
        event = {
            "httpMethod": "GET",
            "path": "/v1/activity/",
            "pathParameters": {},
        }
        response = activity_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 400)

    @patch("activity_service.query_cache_table")
    def test_activity_lookup_database_error(self, mock_query):
        mock_query.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Throughput exceeded"}},
            "Query",
        )
        event = {
            "httpMethod": "GET",
            "path": "/v1/activity/ACC100",
            "pathParameters": {"account_id": "ACC100"},
        }
        response = activity_service.lambda_handler(event, self.context)
        self.assertEqual(response["statusCode"], 502)


if __name__ == "__main__":
    unittest.main()
