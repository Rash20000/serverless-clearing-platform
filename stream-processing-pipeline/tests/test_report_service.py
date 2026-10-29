import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialize mocks & environment variables
import report_service


class TestReportService(unittest.TestCase):
    def test_get_recipient_emails_from_ssm(self):
        mock_ssm_val = "ops@example.com, trading@example.com\n\nfinance@example.com, "
        with patch("report_service.get_ssm_parameter", return_value=mock_ssm_val):
            emails = report_service.get_recipient_emails_from_ssm("/test/emails")
            self.assertEqual(emails, ["ops@example.com", "trading@example.com", "finance@example.com"])

    def test_query_partition(self):
        mock_tbl = MagicMock()
        with patch("report_service.query_all_items", return_value=[{"id": "1"}]) as mock_query:
            res = report_service._query_partition(mock_tbl, "job-x#A", "20261018")
            self.assertEqual(res, [{"id": "1"}])
            mock_query.assert_called_once()

    def test_fetch_report_data_aggregates(self):
        def fake_query(tbl, pk, date_str):
            if pk.endswith("#A"):
                return [{"id": "REC_A"}]
            if pk.endswith("#0"):
                return [{"id": "REC_0"}]
            if pk.endswith("#Z"):
                raise RuntimeError("Partition error")
            return []

        with patch("report_service._query_partition", side_effect=fake_query):
            records = report_service.fetch_report_data("20261018")
            record_ids = {r["id"] for r in records}
            self.assertIn("REC_A", record_ids)
            self.assertIn("REC_0", record_ids)
            self.assertEqual(len(records), 2)

    def test_build_csv(self):
        records = [
            {
                "id": "REC001",
                "status": "PROCESSED",
                "amount": "100.50",
                "processed_at": "2026-10-18T10:00:00Z",
                "reference_id": "REF1",
                "reference_type": "ORDER",
                "reference_status": "ACTIVE",
            }
        ]
        csv_bytes = report_service.build_csv(records)
        csv_str = csv_bytes.decode("utf-8")
        self.assertIn("id,status,amount,processed_at", csv_str)
        self.assertIn("REC001,PROCESSED,100.50,2026-10-18T10:00:00Z", csv_str)

    def test_upload_to_s3(self):
        mock_s3 = MagicMock()
        with patch("report_service.get_s3_client", return_value=mock_s3):
            key = report_service.upload_to_s3(b"header1,header2\n1,2", "20261018")
            self.assertEqual(key, "archive/job-v1/report_20261018.csv")
            mock_s3.put_object.assert_called_once_with(
                Bucket=report_service.ARCHIVE_BUCKET_NAME,
                Key=key,
                Body=b"header1,header2\n1,2",
                ContentType="text/csv",
            )

    def test_deliver_report_email_skipped_missing_config(self):
        with patch.object(report_service, "EMAIL_SSM_PARAM_NAME", ""):
            res = report_service.deliver_report_email(b"csv", "20261018", 10, "archive/key.csv")
            self.assertEqual(res["status"], "SKIPPED")

        with patch.object(report_service, "PINPOINT_APP_ID", ""):
            res = report_service.deliver_report_email(b"csv", "20261018", 10, "archive/key.csv")
            self.assertEqual(res["status"], "SKIPPED")

    def test_deliver_report_email_no_recipients(self):
        with patch("report_service.get_recipient_emails_from_ssm", return_value=[]):
            res = report_service.deliver_report_email(b"csv", "20261018", 10, "archive/key.csv")
            self.assertEqual(res["status"], "SKIPPED")
            self.assertEqual(res["reason"], "No recipients found")

    def test_deliver_report_email_success(self):
        with patch("report_service.get_recipient_emails_from_ssm", return_value=["user@example.com"]), \
             patch("report_service.send_email_via_pinpoint", return_value={"MessageResponse": {"Result": {}}}) as mock_send:
            res = report_service.deliver_report_email(b"csv", "20261018", 5, "archive/report_20261018.csv")
            self.assertIn("MessageResponse", res)
            mock_send.assert_called_once()
            call_kwargs = mock_send.call_args[1]
            self.assertEqual(call_kwargs["to_addresses"], ["user@example.com"])
            self.assertEqual(call_kwargs["attachment_filename"], "report_20261018.csv")

    def test_handler_success(self):
        event = {"date": "20261018"}
        with patch("report_service.fetch_report_data", return_value=[{"id": "R1"}]), \
             patch("report_service.build_csv", return_value=b"id\nR1\n"), \
             patch("report_service.upload_to_s3", return_value="archive/job-v1/report_20261018.csv"), \
             patch("report_service.deliver_report_email", return_value={"status": "SENT"}), \
             patch("report_service.record_job_end") as mock_record:

            res = report_service.handler(event, None)
            self.assertEqual(res["statusCode"], 200)
            self.assertEqual(res["date"], "20261018")
            self.assertEqual(res["record_count"], 1)
            self.assertEqual(res["s3_key"], "archive/job-v1/report_20261018.csv")

            mock_record.assert_called_once_with(
                jobs_table_name=report_service.JOBS_TABLE_NAME,
                function_name="report",
                status="COMPLETED",
                metadata={
                    "date": "20261018",
                    "s3_key": "archive/job-v1/report_20261018.csv",
                    "record_count": 1,
                    "email_status": "SENT",
                },
            )

    def test_handler_failure(self):
        event = {"date": "20261018"}
        with patch("report_service.fetch_report_data", side_effect=RuntimeError("Query error")), \
             patch("report_service.record_job_end") as mock_record:

            with self.assertRaises(RuntimeError):
                report_service.handler(event, None)

            mock_record.assert_called_once_with(
                jobs_table_name=report_service.JOBS_TABLE_NAME,
                function_name="report",
                status="FAILED",
                metadata={"date": "20261018", "error": "Query error"},
            )


if __name__ == "__main__":
    unittest.main()
