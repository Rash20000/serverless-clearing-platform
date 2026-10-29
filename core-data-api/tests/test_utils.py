"""Unit tests for utils.py functions in core-data-api."""

import json
import unittest
from unittest.mock import MagicMock, patch

import tests  # Initialise mocks and sys.path
import utils


class TestUtils(unittest.TestCase):
    @patch("utils.query_cache_table")
    def test_parallel_cache_queries(self, mock_query):
        mock_query.side_effect = lambda **kwargs: [{"queried": kwargs.get("table_name")}]
        queries = [
            {"table_name": "cache.account", "partition_key": "accountId", "partition_value": "ACC1"},
            {"table_name": "cache.activity", "partition_key": "accountId", "partition_value": "ACC1"},
        ]
        results = utils.parallel_cache_queries(queries, max_workers=2)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0]["queried"], "cache.account")
        self.assertEqual(results[1][0]["queried"], "cache.activity")

    @patch("utils._eventbridge_client")
    def test_put_event(self, mock_eb):
        mock_eb.put_events.return_value = {"FailedEntryCount": 0, "Entries": [{"EventId": "ev-1"}]}
        resp = utils.put_event(
            source="inbound.api",
            detail_type="AccountCreated",
            detail={"accountId": "ACC-TEST"},
        )
        self.assertEqual(resp["FailedEntryCount"], 0)
        mock_eb.put_events.assert_called_once()

    @patch("utils._eventbridge_client")
    def test_put_events_batch(self, mock_eb):
        mock_eb.put_events.return_value = {"FailedEntryCount": 0}
        entries = [
            {"source": "test", "detail_type": "type1", "detail": {"id": 1}},
            {"source": "test", "detail_type": "type2", "detail": {"id": 2}},
        ]
        responses = utils.put_events_batch(entries)
        self.assertEqual(len(responses), 1)

    @patch("utils.http_request")
    def test_call_outbound_gateway(self, mock_http):
        mock_http.return_value = {"status": 200, "body": json.dumps({"status": "ok"})}
        resp = utils.call_outbound_gateway(
            path="/v1/quotes",
            payload={"symbol": "AAPL"},
        )
        self.assertEqual(resp["status"], 200)
        mock_http.assert_called_once()


if __name__ == "__main__":
    unittest.main()
