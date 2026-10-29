"""Unit tests for probe_service.py."""

import json
import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
import probe_service


class TestProbeService(unittest.TestCase):
    def test_detect_row_length_standard_lf(self):
        # 30 chars line + \n = 31 bytes
        sample = b"ACC1234567JOHN_DOE_CORP      \nACC1234568JANE_DOE_CORP      \n"
        length = probe_service.detect_row_length(sample)
        self.assertEqual(length, 30)

    def test_detect_row_length_crlf(self):
        # 30 chars line + \r + \n = 31 bytes per line when split by \n
        sample = b"ACC1234567JOHN_DOE_CORP     \r\nACC1234568JANE_DOE_CORP     \r\n"
        length = probe_service.detect_row_length(sample)
        self.assertEqual(length, 30)

    def test_detect_row_length_empty_raises(self):
        with self.assertRaises(ValueError):
            probe_service.detect_row_length(b"")

    def test_detect_row_length_no_newline_raises(self):
        with self.assertRaises(ValueError):
            probe_service.detect_row_length(b"NO_NEWLINE_HERE_JUST_TEXT")

    def test_detect_row_length_malformed_partial_header(self):
        # Realistic operational scenario: S3 file truncated mid-header with no terminal delimiter
        partial_row = b"ACC_TRUNCATED_ROW_FRAGMENT_NO_DELIM"
        with self.assertRaises(ValueError) as ctx:
            probe_service.detect_row_length(partial_row)
        self.assertIn("Sample too small to detect row boundary", str(ctx.exception))

    def test_snap_to_row_boundary_malformed_edge_cases(self):
        # Realistic operational scenario: Chunk range requested beyond file boundary
        # Must snap cleanly to total_size - 1 without generating negative or out-of-range offsets
        self.assertEqual(probe_service._snap_to_row_boundary(150, 20, 100), 99)
        # Exact boundary match
        self.assertEqual(probe_service._snap_to_row_boundary(19, 20, 100), 19)
        # Mid-row byte split (e.g. byte 25 in row 20-39) -> must snap back to row 1 end (19)
        self.assertEqual(probe_service._snap_to_row_boundary(25, 20, 100), 19)

    def test_snap_to_row_boundary(self):
        # row_length = 10. Rows are 0-9, 10-19, 20-29.
        # end_byte = 15 should snap to 9
        self.assertEqual(probe_service._snap_to_row_boundary(15, 10, 100), 9)
        # end_byte = 19 should snap to 19
        self.assertEqual(probe_service._snap_to_row_boundary(19, 10, 100), 19)
        # end_byte = 99 (total_size - 1)
        self.assertEqual(probe_service._snap_to_row_boundary(99, 10, 100), 99)
        # end_byte >= total_size
        self.assertEqual(probe_service._snap_to_row_boundary(150, 10, 100), 99)

    def test_snap_to_row_boundary_invalid_row_length(self):
        with self.assertRaises(ValueError):
            probe_service._snap_to_row_boundary(10, 0, 100)

    def test_build_chunk_manifests_empty(self):
        manifests = probe_service.build_chunk_manifests("b", "k", 0, 10, 5)
        self.assertEqual(manifests, [])

    def test_build_chunk_manifests_multi_chunks(self):
        # total_size = 100, row_length = 10, rows_per_chunk = 5 (50 bytes/chunk)
        manifests = probe_service.build_chunk_manifests("test-b", "test-k", 100, 10, 5)
        self.assertEqual(len(manifests), 2)

        self.assertEqual(manifests[0]["start_byte"], 0)
        self.assertEqual(manifests[0]["end_byte"], 49)
        self.assertEqual(manifests[0]["start_row"], 0)
        self.assertEqual(manifests[0]["end_row"], 4)

        self.assertEqual(manifests[1]["start_byte"], 50)
        self.assertEqual(manifests[1]["end_byte"], 99)
        self.assertEqual(manifests[1]["start_row"], 5)
        self.assertEqual(manifests[1]["end_row"], 9)

    def test_build_chunk_manifests_invalid_params(self):
        with self.assertRaises(ValueError):
            probe_service.build_chunk_manifests("b", "k", 100, -1, 5)
        with self.assertRaises(ValueError):
            probe_service.build_chunk_manifests("b", "k", 100, 10, 0)

    @patch("probe_service.s3_get_byte_range")
    @patch("probe_service.s3_head_object")
    def test_handler_direct_invocation(self, mock_head, mock_range):
        mock_head.return_value = {"ContentLength": 200}
        mock_range.return_value = b"ROW_000000000000000000000000\nROW_000000000000000000000001\n"

        event = {"bucket": "my-bucket", "key": "my-key", "rows_per_chunk": 5}
        context = MagicMock()
        context.aws_request_id = "test-req-12345"

        result = probe_service.handler(event, context)
        self.assertEqual(result["bucket"], "my-bucket")
        self.assertEqual(result["key"], "my-key")
        self.assertEqual(result["total_size"], 200)
        self.assertGreater(result["total_chunks"], 0)

    @patch("probe_service.s3_head_object")
    def test_handler_empty_file(self, mock_head):
        mock_head.return_value = {"ContentLength": 0}
        event = {"bucket": "my-bucket", "key": "empty-file.dat"}
        context = MagicMock()

        result = probe_service.handler(event, context)
        self.assertEqual(result["total_size"], 0)
        self.assertEqual(result["total_chunks"], 0)
        self.assertEqual(result["chunks"], [])

    @patch("probe_service.get_sfn_client")
    @patch("probe_service.s3_get_byte_range")
    @patch("probe_service.s3_head_object")
    def test_handler_s3_event_triggers_sfn(self, mock_head, mock_range, mock_get_sfn):
        mock_head.return_value = {"ContentLength": 100}
        mock_range.return_value = b"ROW_DATA_SAMPLE_001\nROW_DATA_SAMPLE_002\n"
        mock_sfn = MagicMock()
        mock_get_sfn.return_value = mock_sfn

        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "landing-bucket"},
                        "object": {"key": "inbound%2Ffeeds%2Ffeed1.dat"},
                    }
                }
            ],
            "state_machine_arn": "arn:aws:states:us-east-1:123456789012:stateMachine:file-pipeline",
        }
        context = MagicMock()
        context.aws_request_id = "req-98765432"

        result = probe_service.handler(event, context)
        self.assertEqual(result["bucket"], "landing-bucket")
        self.assertEqual(result["key"], "inbound/feeds/feed1.dat")
        mock_sfn.start_execution.assert_called_once()
        call_kwargs = mock_sfn.start_execution.call_args[1]
        self.assertEqual(call_kwargs["stateMachineArn"], event["state_machine_arn"])
        self.assertIn("file-load-req-9876", call_kwargs["name"])


if __name__ == "__main__":
    unittest.main()
