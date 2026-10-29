"""Unit tests for completion_service.py."""

import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
import completion_service


class TestCompletionService(unittest.TestCase):
    def test_aggregate_chunk_results_normal(self):
        map_results = [
            {"records_written": 5000, "records_parsed": 5000},
            {"records_written": 4500, "records_parsed": 4500},
            {"records_written": 200, "records_parsed": 200},
        ]
        stats = completion_service.aggregate_chunk_results(map_results)
        self.assertEqual(stats["total_records_written"], 9700)
        self.assertEqual(stats["total_records_parsed"], 9700)
        self.assertEqual(stats["total_chunks"], 3)

    def test_aggregate_chunk_results_empty(self):
        stats = completion_service.aggregate_chunk_results([])
        self.assertEqual(stats["total_records_written"], 0)
        self.assertEqual(stats["total_records_parsed"], 0)
        self.assertEqual(stats["total_chunks"], 0)

    def test_aggregate_chunk_results_with_malformed_items(self):
        map_results = [
            {"records_written": 100, "records_parsed": 100},
            None,
            "error_string",
            {"records_written": 50, "records_parsed": 50},
        ]
        stats = completion_service.aggregate_chunk_results(map_results)  # type: ignore
        self.assertEqual(stats["total_records_written"], 150)
        self.assertEqual(stats["total_records_parsed"], 150)
        self.assertEqual(stats["total_chunks"], 4)

    @patch("completion_service.emit_end_of_file_marker")
    def test_handler_success(self, mock_emit_eof):
        mock_emit_eof.return_value = {
            "id": "End_Of_File",
            "total_records": 10000,
            "total_chunks": 2,
            "status": "COMPLETED",
        }

        event = {
            "probe_result": {
                "bucket": "landing-bucket",
                "key": "inbound/feeds/data.dat",
                "total_size": 1048576,
                "total_chunks": 2,
            },
            "map_results": [
                {"records_written": 5000, "records_parsed": 5000},
                {"records_written": 5000, "records_parsed": 5000},
            ],
        }

        res = completion_service.handler(event, MagicMock())

        self.assertEqual(res["status"], "COMPLETED")
        self.assertEqual(res["total_records_written"], 10000)
        self.assertEqual(res["total_records_parsed"], 10000)
        self.assertEqual(res["total_chunks"], 2)
        self.assertEqual(res["source_bucket"], "landing-bucket")
        self.assertEqual(res["source_key"], "inbound/feeds/data.dat")
        self.assertEqual(res["reconciliation"]["reconciliation_status"], "BALANCED")

    @patch("completion_service.emit_end_of_file_marker")
    def test_reconciliation_warning_within_tolerance(self, mock_emit_eof):
        mock_emit_eof.return_value = {"id": "End_Of_File", "status": "COMPLETED_WITH_WARNINGS"}

        event = {
            "probe_result": {
                "bucket": "landing-bucket",
                "key": "inbound/feeds/feed.dat",
                "expected_records": 10000,
                "total_size": 1048576,
            },
            "map_results": [
                {"records_written": 4998, "records_parsed": 4998, "records_failed": 2},
                {"records_written": 4998, "records_parsed": 4998, "records_failed": 2},
            ],
        }

        # 4 records failed <= 10 tolerance -> status is COMPLETED_WITH_WARNINGS
        res = completion_service.handler(event, MagicMock())
        self.assertEqual(res["status"], "COMPLETED_WITH_WARNINGS")
        self.assertEqual(res["reconciliation"]["reconciliation_status"], "WARNING_WITHIN_TOLERANCE")
        self.assertEqual(res["reconciliation"]["count_variance"], 4)

    @patch("completion_service.emit_end_of_file_marker")
    def test_reconciliation_critical_breach_halts_execution(self, mock_emit_eof):
        event = {
            "probe_result": {
                "bucket": "landing-bucket",
                "key": "inbound/feeds/feed.dat",
                "expected_records": 10000,
                "total_size": 1048576,
            },
            "map_results": [
                {"records_written": 4900, "records_parsed": 4900, "records_failed": 100},
                {"records_written": 4900, "records_parsed": 4900, "records_failed": 100},
            ],
        }

        # 200 records failed > 10 tolerance -> critical breach raises ValueError to halt Step Functions
        with self.assertRaises(ValueError) as ctx:
            completion_service.handler(event, MagicMock())
        self.assertIn("critical reconciliation breach", str(ctx.exception))

        mock_emit_eof.assert_called_once()
        call_kwargs = mock_emit_eof.call_args[1]
        self.assertEqual(call_kwargs["total_records"], 9800)
        self.assertEqual(call_kwargs["status"], "FAILED")
        self.assertEqual(call_kwargs["total_chunks"], 2)
        self.assertEqual(call_kwargs["metadata"]["source_bucket"], "landing-bucket")


if __name__ == "__main__":
    unittest.main()
