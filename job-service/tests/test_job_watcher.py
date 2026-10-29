"""Unit tests for job_watcher.py."""

import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
import job_watcher


class TestJobWatcher(unittest.TestCase):
    def test_build_failure_alert_html(self):
        item = {
            "PK": "load-end",
            "SK": "20261018#123456",
            "date": "20261018",
            "completed_at": "2026-10-18T12:00:00Z",
            "error": "S3 connection timeout after 3 retries",
        }
        html = job_watcher.build_failure_alert_html(item)
        self.assertIn("load-end", html)
        self.assertIn("20261018#123456", html)
        self.assertIn("20261018", html)
        self.assertIn("S3 connection timeout after 3 retries", html)

    def test_process_stream_record_ignored_pk(self):
        item = {"PK": "other-pk", "status": "COMPLETED"}
        outcome = job_watcher.process_stream_record(item)
        self.assertEqual(outcome, "IGNORED")

    @patch("job_watcher.send_pinpoint_email")
    def test_process_stream_record_failure_alert(self, mock_send_email):
        item = {
            "PK": "load-end",
            "SK": "20261018#999",
            "date": "20261018",
            "status": "FAILED",
            "error": "Fatal data truncation error",
        }
        outcome = job_watcher.process_stream_record(item)
        self.assertEqual(outcome, "FAILURE_ALERTED")
        mock_send_email.assert_called_once()
        call_kwargs = mock_send_email.call_args[1]
        self.assertIn("ALERT: Job Failed (20261018)", call_kwargs["subject"])

    @patch("job_watcher.invoke_lambda_async")
    @patch("job_watcher.lookup_registered_report_job")
    def test_process_stream_record_success_triggers_report(self, mock_lookup, mock_invoke):
        mock_lookup.return_value = {
            "PK": "REPORT_REGISTRY",
            "SK": "daily-summary-report",
            "report_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:report-fn",
            "enabled": True,
        }
        item = {
            "PK": "load-end",
            "SK": "20261018#111",
            "date": "20261018",
            "status": "COMPLETED",
        }
        outcome = job_watcher.process_stream_record(item)
        self.assertEqual(outcome, "SUCCESS_TRIGGERED")
        mock_invoke.assert_called_once()
        call_kwargs = mock_invoke.call_args[1]
        self.assertEqual(call_kwargs["function_name"], "arn:aws:lambda:us-east-1:123456789012:function:report-fn")
        self.assertEqual(call_kwargs["payload"]["date"], "20261018")
        self.assertEqual(call_kwargs["payload"]["trigger"], "load-end")

    @patch("job_watcher.lookup_registered_report_job")
    def test_process_stream_record_success_disabled_registry(self, mock_lookup):
        mock_lookup.return_value = {
            "PK": "REPORT_REGISTRY",
            "SK": "daily-summary-report",
            "report_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:report-fn",
            "enabled": False,
        }
        item = {
            "PK": "load-end",
            "SK": "20261018#111",
            "date": "20261018",
            "status": "COMPLETED",
        }
        outcome = job_watcher.process_stream_record(item)
        self.assertEqual(outcome, "IGNORED")

    @patch("job_watcher.process_stream_record")
    def test_handler_skips_remove_records(self, mock_process):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "dynamodb": {"OldImage": {"PK": {"S": "load-end"}}},
                }
            ]
        }
        res = job_watcher.handler(event, MagicMock())
        self.assertEqual(res["statusCode"], 200)
        self.assertEqual(res["invocations"], 0)
        self.assertEqual(res["failures_handled"], 0)
        mock_process.assert_not_called()

    @patch("job_watcher.process_stream_record")
    def test_handler_aggregates_outcomes(self, mock_process):
        mock_process.side_effect = ["SUCCESS_TRIGGERED", "FAILURE_ALERTED", "IGNORED"]
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"PK": {"S": "load-end"}, "status": {"S": "COMPLETED"}}},
                },
                {
                    "eventName": "MODIFY",
                    "dynamodb": {"NewImage": {"PK": {"S": "load-end"}, "status": {"S": "FAILED"}}},
                },
                {
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"PK": {"S": "other"}, "status": {"S": "COMPLETED"}}},
                },
            ]
        }
        res = job_watcher.handler(event, MagicMock())
        self.assertEqual(res["statusCode"], 200)
        self.assertEqual(res["invocations"], 1)
        self.assertEqual(res["failures_handled"], 1)
        self.assertEqual(mock_process.call_count, 3)


if __name__ == "__main__":
    unittest.main()
