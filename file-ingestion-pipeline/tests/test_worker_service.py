"""Unit tests for worker_service.py."""

import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
import worker_service


class TestWorkerService(unittest.TestCase):
    def test_parse_line_valid(self):
        # Layout:
        # id: 0-10, entity_name: 10-40, status: 40-50, amount: 50-62
        raw_line = "ACC1234567JOHN_DOE_CORP                 ACTIVE    000000150000"
        parsed = worker_service.parse_line(raw_line)
        self.assertEqual(parsed["id"], "ACC1234567")
        self.assertEqual(parsed["entity_name"], "JOHN_DOE_CORP")
        self.assertEqual(parsed["status"], "ACTIVE")
        self.assertEqual(parsed["amount"], "000000150000")

    def test_parse_line_empty(self):
        self.assertEqual(worker_service.parse_line(""), {})

    def test_parse_line_malformed_truncated_row(self):
        # Realistic operational scenario: A corrupted line missing mandatory fixed-width field segments
        # Row layout requires up to index 62, but feed delivers only 15 characters
        truncated_line = "ACC1234567SHORT"
        parsed = worker_service.parse_line(truncated_line)
        # Parser should extract available slices without throwing an IndexError
        self.assertEqual(parsed["id"], "ACC1234567")
        self.assertEqual(parsed["entity_name"], "SHORT")
        self.assertEqual(parsed["status"], "")
        self.assertEqual(parsed["amount"], "")

    def test_parse_line_duplicate_keys_and_whitespace(self):
        # Realistic operational scenario: Padded whitespace across fixed-width columns
        padded_line = "ACC0000099   ACME_HOLDINGS_CORP         APPROVED  000009999999"
        parsed = worker_service.parse_line(padded_line)
        self.assertEqual(parsed["id"], "ACC0000099")
        self.assertEqual(parsed["entity_name"], "ACME_HOLDINGS_CORP")
        self.assertEqual(parsed["status"], "APPROVED")
        self.assertEqual(parsed["amount"], "000009999999")

    def test_parse_lines(self):
        lines = [
            "ACC0000001CORP_A                        ACTIVE    000000010000",
            "   ",  # blank line should be skipped
            "ACC0000002CORP_B                        PENDING   000000020000",
        ]
        parsed = worker_service.parse_lines(lines)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0]["id"], "ACC0000001")
        self.assertEqual(parsed[1]["id"], "ACC0000002")

    def test_decode_and_split(self):
        raw = b"LINE1\n\nLINE2\nLINE3\n"
        lines = worker_service.decode_and_split(raw)
        self.assertEqual(lines, ["LINE1", "LINE2", "LINE3"])

    def test_decode_and_split_empty(self):
        self.assertEqual(worker_service.decode_and_split(b""), [])

    def test_parallel_parse(self):
        lines = [
            f"ACC{i:07d}CORP_{i:<25}ACTIVE    000000100000"
            for i in range(20)
        ]
        parsed = worker_service.parallel_parse(lines, max_workers=4)
        self.assertEqual(len(parsed), 20)
        self.assertEqual(parsed[0]["id"], "ACC0000000")
        self.assertEqual(parsed[19]["id"], "ACC0000019")

    def test_parallel_parse_empty(self):
        self.assertEqual(worker_service.parallel_parse([]), [])

    @patch("worker_service.batch_write_items")
    def test_parallel_write(self, mock_batch_write):
        mock_batch_write.side_effect = lambda table, batch: len(batch)
        records = [{"id": f"ACC{i}"} for i in range(60)]

        written = worker_service.parallel_write("test-table", records, batch_size=25, max_workers=2)
        self.assertEqual(written, 60)
        self.assertEqual(mock_batch_write.call_count, 3)  # 25 + 25 + 10

    def test_parallel_write_empty(self):
        written = worker_service.parallel_write("test-table", [])
        self.assertEqual(written, 0)

    def test_handler_missing_keys_raises(self):
        with self.assertRaises(KeyError):
            worker_service.handler({"bucket": "b"}, MagicMock())

    @patch("worker_service.parallel_write")
    @patch("worker_service.s3_get_byte_range")
    def test_handler_success(self, mock_range, mock_write):
        raw_feed = (
            b"ACC0000001CORP_A                        ACTIVE    000000010000\n"
            b"ACC0000002CORP_B                        ACTIVE    000000020000\n"
        )
        mock_range.return_value = raw_feed
        mock_write.return_value = 2

        event = {
            "bucket": "landing-bucket",
            "key": "inbound/feeds/feed.dat",
            "start_byte": 0,
            "end_byte": len(raw_feed) - 1,
            "start_row": 0,
            "end_row": 1,
        }
        res = worker_service.handler(event, MagicMock())

        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["start_row"], 0)
        self.assertEqual(res["end_row"], 1)
        self.assertEqual(res["records_parsed"], 2)
        self.assertEqual(res["records_written"], 2)


if __name__ == "__main__":
    unittest.main()
