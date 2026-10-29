"""Unit tests for egress_service.py."""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialise mocks and sys.path
import egress_service


class TestEgressService(unittest.TestCase):
    def test_warmup_event(self):
        event = {"action": "warmup", "source": "aws.events"}
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["status"], "warm")

    @patch("egress_service.send_vendor_http_request")
    @patch("egress_service.get_vendor_secret", return_value="primary-api-key-test")
    @patch("egress_service.get_vendor_base_url", return_value="https://api.exchange-primary.example.com/api")
    def test_get_primary_exchange_quote_success(self, mock_url, mock_secret, mock_http):
        mock_http.return_value = {
            "status_code": 200,
            "data": {"lastPrice": 2500.0, "change": 15.5},
        }

        event = {
            "httpMethod": "GET",
            "path": "/market-data/exchange-primary/quote/RELIANCE",
        }
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertEqual(data["exchange"], "PRIMARY")
        self.assertEqual(data["symbol"], "RELIANCE")
        self.assertEqual(data["data"]["lastPrice"], 2500.0)

        mock_http.assert_called_once_with(
            url="https://api.exchange-primary.example.com/api/quote/RELIANCE",
            method="GET",
            headers={"X-Api-Key": "primary-api-key-test"},
        )

    @patch("egress_service.send_vendor_http_request")
    @patch("egress_service.get_vendor_secret")
    @patch("egress_service.get_vendor_base_url", return_value="https://api.exchange-primary.example.com/api")
    def test_get_primary_exchange_quote_401_triggers_secret_refresh_and_retry(self, mock_url, mock_secret, mock_http):
        # First secret call returns stale key, second (force_refresh=True) returns fresh key
        mock_secret.side_effect = ["stale-key", "fresh-rotated-key"]

        # First HTTP call fails with 401, second HTTP call succeeds with 200
        mock_http.side_effect = [
            {"status_code": 401, "data": {"error": "Unauthorized / Key Expired"}},
            {"status_code": 200, "data": {"lastPrice": 2505.0}},
        ]

        event = {
            "httpMethod": "GET",
            "path": "/market-data/exchange-primary/quote/RELIANCE",
        }
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertEqual(data["statusCode"], 200)
        self.assertEqual(data["data"]["lastPrice"], 2505.0)

        # Verify retry happened with fresh rotated key
        self.assertEqual(mock_http.call_count, 2)
        mock_http.assert_called_with(
            url="https://api.exchange-primary.example.com/api/quote/RELIANCE",
            method="GET",
            headers={"X-Api-Key": "fresh-rotated-key"},
        )

    @patch("egress_service.send_vendor_http_request")
    @patch("egress_service.get_vendor_secret", return_value="secondary-api-key-test")
    @patch("egress_service.get_vendor_base_url", return_value="https://api.exchange-secondary.example.com/api")
    def test_get_secondary_exchange_quote_success(self, mock_url, mock_secret, mock_http):
        mock_http.return_value = {
            "status_code": 200,
            "data": {"currentValue": 1500.0},
        }

        event = {
            "httpMethod": "GET",
            "path": "/market-data/exchange-secondary/quote/INFY",
        }
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertEqual(data["exchange"], "SECONDARY")
        self.assertEqual(data["symbol"], "INFY")
        self.assertEqual(data["data"]["currentValue"], 1500.0)

    @patch("egress_service.send_vendor_http_request")
    @patch("egress_service.get_vendor_secret", return_value="ref-vendor-token-999")
    @patch("egress_service.get_vendor_base_url", return_value="https://api.reference-vendor.example.com/v1")
    def test_get_reference_vendor_security_success(self, mock_url, mock_secret, mock_http):
        mock_http.return_value = {
            "status_code": 200,
            "data": {"cusip": "912828B88", "securityType": "Treasury"},
        }

        event = {
            "httpMethod": "GET",
            "path": "/market-data/reference-vendor/security/912828B88",
        }
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertEqual(data["vendor"], "REFERENCE_VENDOR")
        self.assertEqual(data["identifier"], "912828B88")

        mock_http.assert_called_once_with(
            url="https://api.reference-vendor.example.com/v1/securities/912828B88",
            method="GET",
            headers={"Authorization": "Bearer ref-vendor-token-999"},
        )

    @patch("egress_service.get_primary_exchange_quote")
    def test_batch_quotes_primary_success(self, mock_primary):
        mock_primary.side_effect = lambda sym: {"exchange": "PRIMARY", "symbol": sym, "data": {"price": 100}}

        event = {
            "httpMethod": "POST",
            "path": "/market-data/batch",
            "body": json.dumps({"exchange": "PRIMARY", "symbols": ["TCS", "INFY"]}),
        }
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 200)
        data = json.loads(resp["body"])
        self.assertEqual(data["exchange"], "PRIMARY")
        self.assertEqual(data["total_requested"], 2)
        self.assertEqual(len(data["results"]), 2)

    def test_batch_quotes_missing_symbols(self):
        event = {
            "httpMethod": "POST",
            "path": "/market-data/batch",
            "body": json.dumps({"exchange": "PRIMARY", "symbols": []}),
        }
        resp = egress_service.lambda_handler(event, MagicMock())
        self.assertEqual(resp["statusCode"], 400)


if __name__ == "__main__":
    unittest.main()
