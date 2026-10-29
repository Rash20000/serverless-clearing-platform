"""Unit tests for partner-egress-gateway utils.py."""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import utils


class TestEgressUtils(unittest.TestCase):
    def setUp(self):
        utils._secrets_cache.clear()
        utils._ssm_cache.clear()

    def test_client_caching(self):
        sm1 = utils.get_secretsmanager_client()
        sm2 = utils.get_secretsmanager_client()
        self.assertIs(sm1, sm2)

        ssm1 = utils.get_ssm_client()
        ssm2 = utils.get_ssm_client()
        self.assertIs(ssm1, ssm2)

        pool1 = utils.get_http_pool()
        pool2 = utils.get_http_pool()
        self.assertIs(pool1, pool2)

    def test_get_vendor_secret_success_and_cache(self):
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = {"SecretString": "super-secret-key-123"}
        with patch("utils.get_secretsmanager_client", return_value=mock_sm):
            val = utils.get_vendor_secret("exchange-primary")
            self.assertEqual(val, "super-secret-key-123")
            mock_sm.get_secret_value.assert_called_once_with(SecretId="/egress/market-data/exchange-primary/api-key")

            # Second call should use cache
            cached_val = utils.get_vendor_secret("exchange-primary")
            self.assertEqual(cached_val, "super-secret-key-123")
            self.assertEqual(mock_sm.get_secret_value.call_count, 1)

    def test_get_vendor_secret_failure(self):
        mock_sm = MagicMock()
        mock_sm.get_secret_value.side_effect = tests.ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "get_secret_value")
        with patch("utils.get_secretsmanager_client", return_value=mock_sm):
            with self.assertRaises(tests.ClientError):
                utils.get_vendor_secret("UNKNOWN")

    def test_get_vendor_base_url_success_and_cache(self):
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "https://custom-api.vendor.com"}}
        with patch("utils.get_ssm_client", return_value=mock_ssm):
            url = utils.get_vendor_base_url("exchange-secondary")
            self.assertEqual(url, "https://custom-api.vendor.com")
            mock_ssm.get_parameter.assert_called_once_with(Name="/egress/market-data/exchange-secondary/base-url")

            # Second call should use cache
            cached_url = utils.get_vendor_base_url("exchange-secondary")
            self.assertEqual(cached_url, "https://custom-api.vendor.com")
            self.assertEqual(mock_ssm.get_parameter.call_count, 1)

    def test_get_vendor_base_url_fallback_default(self):
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = tests.ClientError({"Error": {"Code": "ParameterNotFound"}}, "get_parameter")
        with patch("utils.get_ssm_client", return_value=mock_ssm):
            url = utils.get_vendor_base_url("exchange-primary", fallback_default="https://fallback.exchange-primary.example.com")
            self.assertEqual(url, "https://fallback.exchange-primary.example.com")

    def test_get_vendor_base_url_missing_raises_without_fallback(self):
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = tests.ClientError({"Error": {"Code": "ParameterNotFound"}}, "get_parameter")
        with patch("utils.get_ssm_client", return_value=mock_ssm):
            with self.assertRaises(tests.ClientError):
                utils.get_vendor_base_url("UNKNOWN", fallback_default="")

    def test_send_vendor_http_request_json(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.data = json.dumps({"symbol": "TCS", "price": 3800.5}).encode("utf-8")

        mock_pool = MagicMock()
        mock_pool.request.return_value = mock_resp

        with patch("utils.get_http_pool", return_value=mock_pool):
            result = utils.send_vendor_http_request(
                url="https://api.test/quote",
                method="GET",
                headers={"X-Test": "1"},
            )
            self.assertEqual(result["status_code"], 200)
            self.assertEqual(result["data"]["symbol"], "TCS")
            self.assertEqual(result["data"]["price"], 3800.5)

    def test_send_vendor_http_request_post_json_body(self):
        mock_resp = MagicMock()
        mock_resp.status = 201
        mock_resp.data = b"OK"

        mock_pool = MagicMock()
        mock_pool.request.return_value = mock_resp

        with patch("utils.get_http_pool", return_value=mock_pool):
            result = utils.send_vendor_http_request(
                url="https://api.test/batch",
                method="POST",
                body={"symbols": ["TCS", "INFY"]},
            )
            self.assertEqual(result["status_code"], 201)
            self.assertEqual(result["data"], {"raw_response": "OK"})


if __name__ == "__main__":
    unittest.main()
