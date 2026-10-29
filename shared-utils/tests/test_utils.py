import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialise mocks and sys.path
import utils


class TestSharedUtils(unittest.TestCase):
    @patch("utils.boto3.client")
    def test_client_caching(self, mock_client):
        utils._s3_client = None
        c1 = utils.get_s3_client()
        c2 = utils.get_s3_client()
        self.assertIs(c1, c2)

    @patch("utils.boto3.resource")
    def test_resource_caching(self, mock_resource):
        utils._s3_resource = None
        r1 = utils.get_s3_resource()
        r2 = utils.get_s3_resource()
        self.assertIs(r1, r2)

    @patch("urllib.request.urlopen")
    def test_send_cfn_response_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        event = {
            "ResponseURL": "https://cloudformation-custom-resource-response.s3.amazonaws.com/test",
            "StackId": "arn:aws:cloudformation:us-east-1:123:stack/test",
            "RequestId": "req-123",
            "LogicalResourceId": "MyResource",
        }
        utils.send_cfn_response(
            event=event,
            context=MagicMock(),
            response_status="SUCCESS",
            response_data={"Key": "Value"},
            physical_resource_id="phys-123",
        )
        mock_urlopen.assert_called_once()

    def test_send_cfn_response_missing_url(self):
        # Should not raise exception
        utils.send_cfn_response(
            event={},
            context=MagicMock(),
            response_status="SUCCESS",
        )


if __name__ == "__main__":
    unittest.main()
