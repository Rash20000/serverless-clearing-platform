import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialize mocks & environment variables
import load_consumer_service


class TestLoadConsumerService(unittest.TestCase):
    def test_deserialize_stream_record_valid(self):
        record = {
            "eventID": "evt-1",
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "id": {"S": "REC001A"},
                    "amount": {"N": "150.50"},
                    "status": {"S": "ACTIVE"},
                }
            },
        }
        res = load_consumer_service.deserialize_stream_record(record)
        self.assertEqual(res["id"], "REC001A")
        self.assertEqual(res["amount"], 150.50)
        self.assertEqual(res["status"], "ACTIVE")

    def test_handler_partial_batch_failure_and_poison_pill_isolation(self):
        # Realistic operational scenario: Batch of 3 stream records where the 2nd has a poisoned NewImage
        # Handlers must return batchItemFailures with the failed sequence number without crashing the batch
        stream_event = {
            "Records": [
                {
                    "eventID": "evt-101",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "SequenceNumber": "seq-001",
                        "NewImage": {"id": {"S": "REC101A"}, "amount": {"N": "500"}},
                    },
                },
                {
                    "eventID": "evt-102",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "SequenceNumber": "seq-poison-002",
                        # Malformed NewImage structure causing TypeDeserializer exception
                        "NewImage": {"id": {"INVALID_TYPE": 12345}},
                    },
                },
                {
                    "eventID": "evt-103",
                    "eventName": "INSERT",
                    "dynamodb": {
                        "SequenceNumber": "seq-003",
                        "NewImage": {"id": {"S": "REC103C"}, "amount": {"N": "750"}},
                    },
                },
            ]
        }

        with patch("load_consumer_service.enrich_record", side_effect=lambda r: r), \
             patch("load_consumer_service.batch_write_items"):
            res = load_consumer_service.handler(stream_event, None)
            self.assertEqual(res["statusCode"], 200)
            self.assertEqual(res["processed_records"], 2)
            # Confirms poisoned record seq-poison-002 is captured for AWS selective retry
            self.assertEqual(res["batchItemFailures"], [{"itemIdentifier": "seq-poison-002"}])

    def test_deserialize_stream_record_missing_image(self):
        record = {"eventID": "evt-2", "eventName": "REMOVE"}
        res = load_consumer_service.deserialize_stream_record(record)
        self.assertEqual(res, {})

    def test_enrich_record_end_of_file_marker(self):
        rec = {"id": "End_Of_File", "file_name": "test.dat"}
        res = load_consumer_service.enrich_record(rec)
        self.assertEqual(res, rec)

    def test_enrich_record_empty_reference_table(self):
        with patch.object(load_consumer_service, "REFERENCE_TABLE_NAME", ""):
            rec = {"id": "REC001A", "amount": 100}
            res = load_consumer_service.enrich_record(rec)
            self.assertEqual(res, rec)

    def test_enrich_record_success(self):
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table
        ref_items = [{"PK": "REC001A", "SK": "REF#MASTER", "company": "Acme Corp", "tier": "Gold"}]

        with patch("load_consumer_service.get_dynamodb_resource", return_value=mock_ddb), \
             patch("load_consumer_service.query_all_items", return_value=ref_items):
            rec = {"id": "REC001A", "amount": 100}
            res = load_consumer_service.enrich_record(rec)
            self.assertEqual(res["company"], "Acme Corp")
            self.assertEqual(res["tier"], "Gold")
            self.assertEqual(res["amount"], 100)

    def test_enrich_record_error_fallback(self):
        mock_ddb = MagicMock()
        mock_table = MagicMock()
        mock_ddb.Table.return_value = mock_table

        with patch("load_consumer_service.get_dynamodb_resource", return_value=mock_ddb), \
             patch("load_consumer_service.query_all_items", side_effect=RuntimeError("DDB error")):
            rec = {"id": "REC001A", "amount": 100}
            res = load_consumer_service.enrich_record(rec)
            self.assertEqual(res, rec)

    def test_transform_to_cache_item(self):
        rec = {"id": "TEST1234Z", "amount": 250, "status": "PENDING"}
        with patch("load_consumer_service.get_current_date_string", return_value="20261018"):
            item = load_consumer_service.transform_to_cache_item(rec)
            self.assertEqual(item["PK"], f"{load_consumer_service.JOB_PREFIX}#Z")
            self.assertEqual(item["SK"], "20261018#TEST1234Z")
            self.assertEqual(item["id"], "TEST1234Z")
            self.assertEqual(item["amount"], "250")
            self.assertEqual(item["status"], "PENDING")
            self.assertIn("processed_at", item)

    def test_transform_to_cache_item_empty_id(self):
        rec = {}
        with patch("load_consumer_service.get_current_date_string", return_value="20261018"):
            item = load_consumer_service.transform_to_cache_item(rec)
            self.assertEqual(item["PK"], f"{load_consumer_service.JOB_PREFIX}#X")
            self.assertEqual(item["SK"], "20261018#")

    def test_handler_processes_records_and_writes_cache(self):
        stream_event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "dynamodb": {"OldImage": {"id": {"S": "DEL1"}}},
                },
                {
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"id": {"S": "ITEM01A"}, "amount": {"N": "100"}}},
                },
                {
                    "eventName": "MODIFY",
                    "dynamodb": {"NewImage": {"id": {"S": "ITEM02B"}, "amount": {"N": "200"}}},
                },
            ]
        }

        with patch("load_consumer_service.enrich_record", side_effect=lambda r: {**r, "enriched": True}), \
             patch("load_consumer_service.batch_write_items") as mock_batch_write, \
             patch("load_consumer_service.record_job_end") as mock_record_job:

            res = load_consumer_service.handler(stream_event, None)
            self.assertEqual(res["statusCode"], 200)
            self.assertEqual(res["processed_records"], 2)
            self.assertFalse(res["end_of_file_detected"])

            mock_batch_write.assert_called_once()
            args = mock_batch_write.call_args[0]
            self.assertEqual(args[0], load_consumer_service.CACHE_REPORT_TABLE_NAME)
            self.assertEqual(len(args[1]), 2)
            mock_record_job.assert_not_called()

    def test_handler_detects_end_of_file_and_records_job_end(self):
        stream_event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"id": {"S": "REC999C"}, "amount": {"N": "50"}}},
                },
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "pk": {"S": "End_Of_File"},
                            "file_name": {"S": "source_data.csv"},
                            "total_records": {"N": "1000"},
                        }
                    },
                },
            ]
        }

        with patch("load_consumer_service.enrich_record", side_effect=lambda r: r), \
             patch("load_consumer_service.batch_write_items") as mock_batch_write, \
             patch("load_consumer_service.record_job_end") as mock_record_job:

            res = load_consumer_service.handler(stream_event, None)
            self.assertEqual(res["statusCode"], 200)
            self.assertEqual(res["processed_records"], 1)
            self.assertTrue(res["end_of_file_detected"])

            mock_batch_write.assert_called_once()
            mock_record_job.assert_called_once_with(
                jobs_table_name=load_consumer_service.JOBS_TABLE_NAME,
                function_name="load",
                status="COMPLETED",
                metadata={
                    "marker": "End_Of_File",
                    "processed_records_in_final_batch": 1,
                    "file_metadata": {
                        "pk": "End_Of_File",
                        "file_name": "source_data.csv",
                        "total_records": 1000,
                    },
                },
            )


if __name__ == "__main__":
    unittest.main()
