import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialize mocks & environment variables
import utils


class TestUtils(unittest.TestCase):
    def setUp(self):
        # Reset cached clients and ssm cache
        utils._dynamodb_resource = None
        utils._dynamodb_client = None
        utils._ssm_client = None
        utils._pinpoint_client = None
        utils._s3_client = None
        utils._ssm_cache.clear()

    def test_client_getters_cached(self):
        with patch("boto3.resource") as mock_res, patch("boto3.client") as mock_cli:
            r1 = utils.get_dynamodb_resource()
            r2 = utils.get_dynamodb_resource()
            self.assertIs(r1, r2)
            mock_res.assert_called_once()

            c1 = utils.get_dynamodb_client()
            c2 = utils.get_dynamodb_client()
            self.assertIs(c1, c2)

            s1 = utils.get_ssm_client()
            s2 = utils.get_ssm_client()
            self.assertIs(s1, s2)

            p1 = utils.get_pinpoint_client()
            p2 = utils.get_pinpoint_client()
            self.assertIs(p1, p2)

            s3_1 = utils.get_s3_client()
            s3_2 = utils.get_s3_client()
            self.assertIs(s3_1, s3_2)

    def test_get_env(self):
        with patch.dict(os.environ, {"MY_TEST_KEY": "val123"}):
            self.assertEqual(utils.get_env("MY_TEST_KEY"), "val123")
        self.assertEqual(utils.get_env("NON_EXISTENT_KEY", "fallback"), "fallback")
        with self.assertRaises(EnvironmentError):
            utils.get_env("STILL_NON_EXISTENT_KEY")

    def test_get_current_date_string(self):
        d = utils.get_current_date_string()
        self.assertEqual(len(d), 8)
        self.assertTrue(d.isdigit())

    def test_chunk_list(self):
        items = list(range(10))
        chunks = utils.chunk_list(items, 3)
        self.assertEqual(chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

        with self.assertRaises(ValueError):
            utils.chunk_list(items, 0)

    def test_get_ssm_parameter(self):
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "test@example.com"}}
        with patch("utils.get_ssm_client", return_value=mock_ssm):
            val = utils.get_ssm_parameter("/test/param")
            self.assertEqual(val, "test@example.com")
            mock_ssm.get_parameter.assert_called_once_with(Name="/test/param", WithDecryption=False)

            # Cached call should not call get_parameter again
            cached_val = utils.get_ssm_parameter("/test/param")
            self.assertEqual(cached_val, "test@example.com")
            self.assertEqual(mock_ssm.get_parameter.call_count, 1)

    def test_get_ssm_parameter_error(self):
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = tests.ClientError({"Error": {"Code": "400"}}, "get_parameter")
        with patch("utils.get_ssm_client", return_value=mock_ssm):
            with self.assertRaises(tests.ClientError):
                utils.get_ssm_parameter("/test/error")

    def test_record_job_end_missing_table(self):
        res = utils.record_job_end("", "load", "COMPLETED")
        self.assertEqual(res, {})

    def test_record_job_end_success(self):
        mock_table = MagicMock()
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table
        with patch("utils.get_dynamodb_resource", return_value=mock_ddb):
            res = utils.record_job_end(
                jobs_table_name="my-jobs",
                function_name="load",
                status="COMPLETED",
                metadata={"test_field": "test_val"},
            )
            self.assertEqual(res["PK"], "load-end")
            self.assertEqual(res["status"], "COMPLETED")
            self.assertEqual(res["test_field"], "test_val")
            mock_table.put_item.assert_called_once()

    def test_record_job_end_error(self):
        mock_table = MagicMock()
        mock_table.put_item.side_effect = tests.ClientError({"Error": {"Code": "500"}}, "put_item")
        mock_ddb = MagicMock()
        mock_ddb.Table.return_value = mock_table
        with patch("utils.get_dynamodb_resource", return_value=mock_ddb):
            with self.assertRaises(tests.ClientError):
                utils.record_job_end("my-jobs", "load", "FAILED")

    def test_query_all_items_pagination(self):
        mock_table = MagicMock()
        mock_table.query.side_effect = [
            {"Items": [{"id": "1"}, {"id": "2"}], "LastEvaluatedKey": {"id": "2"}},
            {"Items": [{"id": "3"}]},
        ]
        items = utils.query_all_items(
            table=mock_table,
            key_condition_expression=MagicMock(),
            expression_attribute_values={"val": 1},
            index_name="test-idx",
            projection_expression="id",
        )
        self.assertEqual(len(items), 3)
        self.assertEqual(mock_table.query.call_count, 2)

    def test_query_all_items_by_table_name_string(self):
        mock_ddb = MagicMock()
        mock_tbl = MagicMock()
        mock_tbl.query.return_value = {"Items": [{"id": "A"}]}
        mock_ddb.Table.return_value = mock_tbl
        with patch("utils.get_dynamodb_resource", return_value=mock_ddb):
            items = utils.query_all_items(
                table="table-string",
                key_condition_expression=MagicMock(),
            )
            self.assertEqual(len(items), 1)
            mock_ddb.Table.assert_called_with("table-string")

    def test_batch_write_items_empty(self):
        self.assertEqual(utils.batch_write_items("table", []), 0)

    def test_batch_write_items_success(self):
        mock_ddb = MagicMock()
        mock_client = MagicMock()
        mock_client.batch_write_item.return_value = {"UnprocessedItems": {}}
        mock_ddb.meta.client = mock_client
        with patch("utils.get_dynamodb_resource", return_value=mock_ddb):
            items = [{"id": f"item-{i}"} for i in range(30)]
            written = utils.batch_write_items("test-tbl", items, batch_size=25)
            self.assertEqual(written, 30)
            self.assertEqual(mock_client.batch_write_item.call_count, 2)

    def test_batch_write_items_unprocessed_failure(self):
        mock_ddb = MagicMock()
        mock_client = MagicMock()
        mock_client.batch_write_item.return_value = {"UnprocessedItems": {"test-tbl": [{"PutRequest": {"Item": {}}}]}}
        mock_ddb.meta.client = mock_client
        with patch("utils.get_dynamodb_resource", return_value=mock_ddb):
            with self.assertRaises(RuntimeError):
                utils.batch_write_items("test-tbl", [{"id": "1"}])

    def test_send_email_via_pinpoint_skipped(self):
        res1 = utils.send_email_via_pinpoint("", "from@example.com", ["to@example.com"], "Subject", "Body")
        self.assertEqual(res1["status"], "SKIPPED")
        res2 = utils.send_email_via_pinpoint("app-id", "from@example.com", [], "Subject", "Body")
        self.assertEqual(res2["status"], "SKIPPED")

    def test_send_email_via_pinpoint_success(self):
        mock_pinpoint = MagicMock()
        mock_pinpoint.send_messages.return_value = {"MessageResponse": {"Result": {"to@example.com": {"StatusCode": 200}}}}
        with patch("utils.get_pinpoint_client", return_value=mock_pinpoint):
            res = utils.send_email_via_pinpoint(
                application_id="app-123",
                from_address="sender@example.com",
                to_addresses=["to1@example.com", "to2@example.com"],
                subject="Test Report",
                html_body="<p>Test Body</p>",
                attachment_bytes=b"id,val\n1,100\n",
                attachment_filename="test.csv",
            )
            self.assertIn("MessageResponse", res)
            mock_pinpoint.send_messages.assert_called_once()
            call_kwargs = mock_pinpoint.send_messages.call_args[1]
            self.assertEqual(call_kwargs["ApplicationId"], "app-123")
            self.assertIn("Addresses", call_kwargs["MessageRequest"])

    def test_send_email_via_pinpoint_error(self):
        mock_pinpoint = MagicMock()
        mock_pinpoint.send_messages.side_effect = tests.ClientError({"Error": {"Code": "AccessDenied"}}, "send_messages")
        with patch("utils.get_pinpoint_client", return_value=mock_pinpoint):
            with self.assertRaises(tests.ClientError):
                utils.send_email_via_pinpoint(
                    application_id="app-123",
                    from_address="sender@example.com",
                    to_addresses=["to@example.com"],
                    subject="Test Report",
                    html_body="<p>Body</p>",
                )


if __name__ == "__main__":
    unittest.main()
