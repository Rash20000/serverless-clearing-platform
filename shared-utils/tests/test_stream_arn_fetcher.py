import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import stream_arn_fetcher


class TestStreamArnFetcher(unittest.TestCase):
    @patch("stream_arn_fetcher.get_dynamodb_client")
    def test_fetch_latest_stream_arn_success(self, mock_get_ddb):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "TableName": "test-table",
                "LatestStreamArn": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table/stream/2026-10-18T00:00:00.000",
            }
        }
        mock_get_ddb.return_value = mock_client

        arn = stream_arn_fetcher.fetch_latest_stream_arn("test-table")
        self.assertIn("/stream/2026-10-18", arn)
        mock_client.describe_table.assert_called_once_with(TableName="test-table")

    @patch("stream_arn_fetcher.get_dynamodb_client")
    def test_fetch_latest_stream_arn_missing_stream_raises(self, mock_get_ddb):
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "TableName": "test-table",
            }
        }
        mock_get_ddb.return_value = mock_client

        with self.assertRaises(ValueError):
            stream_arn_fetcher.fetch_latest_stream_arn("test-table")

    def test_handler_delete_success(self):
        event = {"RequestType": "Delete", "ResourceProperties": {"TableName": "my-table"}}
        stream_arn_fetcher.lambda_handler(event, MagicMock())

    @patch("stream_arn_fetcher.fetch_latest_stream_arn")
    def test_handler_create_success(self, mock_fetch):
        mock_fetch.return_value = "arn:aws:dynamodb:us-east-1:123:table/tbl/stream/now"
        event = {"RequestType": "Create", "ResourceProperties": {"TableName": "my-table"}}
        stream_arn_fetcher.lambda_handler(event, MagicMock())
        mock_fetch.assert_called_once_with("my-table")
        self.assertEqual(stream_arn_fetcher.helper.Data["StreamArn"], "arn:aws:dynamodb:us-east-1:123:table/tbl/stream/now")

    def test_handler_missing_table_name_fails(self):
        event = {"RequestType": "Create", "ResourceProperties": {"TableName": ""}}
        with self.assertRaises(ValueError):
            stream_arn_fetcher.lambda_handler(event, MagicMock())

    @patch("stream_arn_fetcher.fetch_latest_stream_arn")
    def test_handler_describe_table_error_fails(self, mock_fetch):
        mock_fetch.side_effect = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable")
        event = {"RequestType": "Create", "ResourceProperties": {"TableName": "non-existent"}}
        with self.assertRaises(ClientError):
            stream_arn_fetcher.lambda_handler(event, MagicMock())


if __name__ == "__main__":
    unittest.main()
