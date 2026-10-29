"""Unit tests for job-service utils.py."""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import utils


class TestJobServiceUtils(unittest.TestCase):
    def test_get_env_existing(self):
        with patch.dict(os.environ, {"JOB_ENV_VAR": "my_val"}):
            self.assertEqual(utils.get_env("JOB_ENV_VAR"), "my_val")

    def test_get_env_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(utils.get_env("NON_EXISTENT", "def_val"), "def_val")

    def test_get_env_missing_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                utils.get_env("NON_EXISTENT")

    @patch("utils.get_pinpoint_client")
    def test_send_pinpoint_email_success(self, mock_get_pinpoint):
        mock_client = MagicMock()
        mock_client.send_messages.return_value = {"Response": "OK"}
        mock_get_pinpoint.return_value = mock_client

        res = utils.send_pinpoint_email(
            app_id="app-123",
            from_email="sender@example.com",
            to_email="recipient@example.com",
            subject="Test Subject",
            html_body="<p>Test Body</p>",
        )
        self.assertEqual(res, {"Response": "OK"})
        mock_client.send_messages.assert_called_once()

    def test_send_pinpoint_email_missing_app_id(self):
        res = utils.send_pinpoint_email(
            app_id="",
            from_email="sender@example.com",
            to_email="recipient@example.com",
            subject="Subject",
            html_body="Body",
        )
        self.assertIsNone(res)

    def test_send_pinpoint_email_missing_emails(self):
        res = utils.send_pinpoint_email(
            app_id="app-123",
            from_email="",
            to_email="",
            subject="Subject",
            html_body="Body",
        )
        self.assertIsNone(res)

    @patch("utils.get_pinpoint_client")
    def test_send_pinpoint_email_client_error(self, mock_get_pinpoint):
        mock_client = MagicMock()
        mock_client.send_messages.side_effect = ClientError({"Error": {"Code": "500"}}, "SendMessages")
        mock_get_pinpoint.return_value = mock_client

        res = utils.send_pinpoint_email(
            app_id="app-123",
            from_email="sender@example.com",
            to_email="recipient@example.com",
            subject="Subject",
            html_body="Body",
        )
        self.assertIsNone(res)

    @patch("utils.get_dynamodb_resource")
    def test_lookup_registered_report_job_found(self, mock_get_ddb):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "PK": "REPORT_REGISTRY",
                "SK": "daily-summary-report",
                "report_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:report-fn",
                "enabled": True,
            }
        }
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        mock_get_ddb.return_value = mock_resource

        item = utils.lookup_registered_report_job("test-table", "daily-summary-report")
        self.assertIsNotNone(item)
        self.assertTrue(item["enabled"])
        mock_table.get_item.assert_called_once_with(
            Key={"PK": "REPORT_REGISTRY", "SK": "daily-summary-report"}
        )

    @patch("utils.get_dynamodb_resource")
    def test_lookup_registered_report_job_not_found(self, mock_get_ddb):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        mock_get_ddb.return_value = mock_resource

        item = utils.lookup_registered_report_job("test-table", "missing-job")
        self.assertIsNone(item)

    def test_lookup_registered_report_job_empty_args(self):
        self.assertIsNone(utils.lookup_registered_report_job("", "job-id"))
        self.assertIsNone(utils.lookup_registered_report_job("table", ""))

    @patch("utils.get_dynamodb_resource")
    def test_lookup_registered_report_job_client_error(self, mock_get_ddb):
        mock_table = MagicMock()
        mock_table.get_item.side_effect = ClientError({"Error": {"Code": "ResourceNotFound"}}, "GetItem")
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        mock_get_ddb.return_value = mock_resource

        item = utils.lookup_registered_report_job("test-table", "job-id")
        self.assertIsNone(item)

    @patch("utils.get_lambda_client")
    def test_invoke_lambda_async_success(self, mock_get_lambda):
        mock_client = MagicMock()
        mock_client.invoke.return_value = {"StatusCode": 202}
        mock_get_lambda.return_value = mock_client

        res = utils.invoke_lambda_async("my-function", {"test": "data"})
        self.assertEqual(res["StatusCode"], 202)
        mock_client.invoke.assert_called_once_with(
            FunctionName="my-function",
            InvocationType="Event",
            Payload=json.dumps({"test": "data"}),
        )

    def test_invoke_lambda_async_empty_name(self):
        res = utils.invoke_lambda_async("", {"test": "data"})
        self.assertIsNone(res)

    @patch("urllib.request.urlopen")
    def test_send_cfn_response_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        event = {
            "ResponseURL": "https://cloudformation-custom-resource-response.s3.amazonaws.com/test",
            "StackId": "arn:aws:cloudformation:us-east-1:123:stack/test",
            "RequestId": "req-123",
            "LogicalResourceId": "MyResource",
        }
        utils.send_cfn_response(event, MagicMock(), "SUCCESS", {"Key": "Val"}, "phys-id-1")
        mock_urlopen.assert_called_once()

    def test_send_cfn_response_missing_url(self):
        # Should not raise exception
        utils.send_cfn_response({}, MagicMock(), "SUCCESS")


if __name__ == "__main__":
    unittest.main()
