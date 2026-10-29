"""Unit tests for report_registrar.py with crhelper."""

import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
import report_registrar


class TestReportRegistrar(unittest.TestCase):
    @patch("report_registrar.get_dynamodb_resource")
    def test_register_job(self, mock_get_ddb):
        mock_table = MagicMock()
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        mock_get_ddb.return_value = mock_resource

        item = report_registrar.register_job(
            table_name="my-jobs",
            job_id="daily-summary-report",
            report_function_arn="arn:aws:lambda:us-east-1:123456789012:function:report-fn",
            description="Daily ledger export",
            registered_at="2026-10-18T10:00:00Z",
        )
        self.assertEqual(item["PK"], "REPORT_REGISTRY")
        self.assertEqual(item["SK"], "daily-summary-report")
        self.assertTrue(item["enabled"])
        self.assertEqual(item["trigger_key"], "load-end")
        mock_table.put_item.assert_called_once_with(Item=item)

    @patch("report_registrar.get_dynamodb_resource")
    def test_unregister_job(self, mock_get_ddb):
        mock_table = MagicMock()
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        mock_get_ddb.return_value = mock_resource

        report_registrar.unregister_job("my-jobs", "daily-summary-report")
        mock_table.delete_item.assert_called_once_with(
            Key={"PK": "REPORT_REGISTRY", "SK": "daily-summary-report"}
        )

    @patch("report_registrar.register_job")
    def test_handler_create(self, mock_register):
        event = {
            "RequestType": "Create",
            "ResourceProperties": {
                "JobsTableName": "my-jobs-table",
                "JobId": "summary-report-v1",
                "ReportFunctionArn": "arn:aws:lambda:us-east-1:123:function:report",
                "Description": "Test report",
            },
        }
        report_registrar.lambda_handler(event, MagicMock())
        mock_register.assert_called_once()

    @patch("report_registrar.unregister_job")
    def test_handler_delete(self, mock_unregister):
        event = {
            "RequestType": "Delete",
            "ResourceProperties": {
                "JobsTableName": "my-jobs-table",
                "JobId": "summary-report-v1",
            },
        }
        report_registrar.lambda_handler(event, MagicMock())
        mock_unregister.assert_called_once_with("my-jobs-table", "summary-report-v1")

    def test_handler_missing_properties_fails(self):
        event = {
            "RequestType": "Create",
            "ResourceProperties": {
                "JobsTableName": "",
                "JobId": "",
            },
        }
        with self.assertRaises(ValueError):
            report_registrar.lambda_handler(event, MagicMock())


if __name__ == "__main__":
    unittest.main()
