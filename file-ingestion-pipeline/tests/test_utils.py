"""Unit tests for utils.py."""

import unittest
from unittest.mock import MagicMock, patch
import os

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import utils


class TestUtils(unittest.TestCase):
    def test_get_env_existing(self):
        with patch.dict(os.environ, {"MY_TEST_VAR": "value123"}):
            self.assertEqual(utils.get_env("MY_TEST_VAR"), "value123")

    def test_get_env_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(utils.get_env("NON_EXISTENT_VAR", "default_val"), "default_val")

    def test_get_env_missing_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                utils.get_env("NON_EXISTENT_VAR")

    def test_partition_list_basic(self):
        items = list(range(10))
        chunks = utils.partition_list(items, 3)
        self.assertEqual(chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

    def test_partition_list_exact_multiple(self):
        items = [1, 2, 3, 4]
        chunks = utils.partition_list(items, 2)
        self.assertEqual(chunks, [[1, 2], [3, 4]])

    def test_partition_list_empty(self):
        self.assertEqual(utils.partition_list([], 5), [])

    def test_partition_list_invalid_size(self):
        with self.assertRaises(ValueError):
            utils.partition_list([1, 2], 0)

    @patch("utils.get_ssm_client")
    def test_get_ssm_parameter_success(self, mock_get_ssm):
        mock_client = MagicMock()
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "secret_param"}}
        mock_get_ssm.return_value = mock_client

        val = utils.get_ssm_parameter("/my/param", decrypt=True)
        self.assertEqual(val, "secret_param")
        mock_client.get_parameter.assert_called_once_with(Name="/my/param", WithDecryption=True)

    @patch("utils.get_ssm_client")
    def test_get_ssm_parameter_failure(self, mock_get_ssm):
        mock_client = MagicMock()
        mock_client.get_parameter.side_effect = ClientError({"Error": {"Code": "404"}}, "GetParameter")
        mock_get_ssm.return_value = mock_client

        with self.assertRaises(ClientError):
            utils.get_ssm_parameter("/bad/param")

    @patch("utils.get_s3_client")
    def test_s3_head_object_success(self, mock_get_s3):
        mock_client = MagicMock()
        mock_client.head_object.return_value = {"ContentLength": 4096}
        mock_get_s3.return_value = mock_client

        res = utils.s3_head_object("test-bucket", "test-key")
        self.assertEqual(res["ContentLength"], 4096)
        mock_client.head_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")

    @patch("utils.get_s3_client")
    def test_s3_get_byte_range_success(self, mock_get_s3):
        mock_client = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = b"sample_range_data"
        mock_client.get_object.return_value = {"Body": mock_body}
        mock_get_s3.return_value = mock_client

        data = utils.s3_get_byte_range("test-bucket", "test-key", 0, 100)
        self.assertEqual(data, b"sample_range_data")
        mock_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key", Range="bytes=0-100"
        )

    @patch("utils._get_table")
    def test_batch_write_items_empty(self, mock_get_table):
        count = utils.batch_write_items("table", [])
        self.assertEqual(count, 0)
        mock_get_table.assert_not_called()

    @patch("utils._get_table")
    def test_batch_write_items_success(self, mock_get_table):
        mock_table = MagicMock()
        mock_writer = MagicMock()
        mock_table.batch_writer.return_value.__enter__.return_value = mock_writer
        mock_get_table.return_value = mock_table

        items = [{"id": "1"}, {"id": "2"}]
        written = utils.batch_write_items("table", items)
        self.assertEqual(written, 2)
        self.assertEqual(mock_writer.put_item.call_count, 2)

    @patch("utils.batch_write_items")
    def test_parallel_batch_write(self, mock_batch_write):
        mock_batch_write.side_effect = [2, 2]
        batches = [[{"id": "1"}, {"id": "2"}], [{"id": "3"}, {"id": "4"}]]
        total = utils.parallel_batch_write("table", batches, max_workers=2)
        self.assertEqual(total, 4)

    @patch("utils._get_table")
    def test_emit_end_of_file_marker_success(self, mock_get_table):
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        res = utils.emit_end_of_file_marker(
            table_name="my-table",
            total_records=5000,
            total_chunks=10,
            metadata={"source_bucket": "b", "source_key": "k"},
        )
        self.assertEqual(res["id"], "End_Of_File")
        self.assertEqual(res["total_records"], 5000)
        self.assertEqual(res["total_chunks"], 10)
        self.assertEqual(res["status"], "COMPLETED")
        self.assertEqual(res["source_bucket"], "b")
        mock_table.put_item.assert_called_once()


if __name__ == "__main__":
    unittest.main()
