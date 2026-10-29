import os
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import tests  # Initialise mocks and sys.path
from botocore.exceptions import ClientError
import empty_bucket


class TestEmptyBucket(unittest.TestCase):
    @patch("empty_bucket.get_s3_resource")
    def test_purge_s3_bucket(self, mock_get_s3):
        mock_bucket = MagicMock()
        mock_resource = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket
        mock_get_s3.return_value = mock_resource

        empty_bucket.purge_s3_bucket("test-bucket")
        mock_bucket.object_versions.delete.assert_called_once()
        mock_bucket.objects.all.return_value.delete.assert_called_once()

    @patch("empty_bucket.purge_s3_bucket")
    def test_handler_create_skips_purge(self, mock_purge):
        event = {"RequestType": "Create", "ResourceProperties": {"BucketName": "b"}}
        empty_bucket.lambda_handler(event, MagicMock())
        mock_purge.assert_not_called()

    @patch("empty_bucket.purge_s3_bucket")
    def test_handler_update_skips_purge(self, mock_purge):
        event = {"RequestType": "Update", "ResourceProperties": {"BucketName": "b"}}
        empty_bucket.lambda_handler(event, MagicMock())
        mock_purge.assert_not_called()

    @patch("empty_bucket.purge_s3_bucket")
    def test_handler_delete_purges_bucket(self, mock_purge):
        event = {"RequestType": "Delete", "ResourceProperties": {"BucketName": "target-bucket"}}
        empty_bucket.lambda_handler(event, MagicMock())
        mock_purge.assert_called_once_with("target-bucket")

    @patch("empty_bucket.purge_s3_bucket")
    def test_handler_delete_empty_bucket_name(self, mock_purge):
        event = {"RequestType": "Delete", "ResourceProperties": {"BucketName": ""}}
        empty_bucket.lambda_handler(event, MagicMock())
        mock_purge.assert_not_called()

    @patch("empty_bucket.purge_s3_bucket")
    def test_handler_delete_no_such_bucket_success(self, mock_purge):
        mock_purge.side_effect = ClientError({"Error": {"Code": "NoSuchBucket"}}, "Delete")
        event = {"RequestType": "Delete", "ResourceProperties": {"BucketName": "non-existent"}}
        # Should not raise exception
        empty_bucket.lambda_handler(event, MagicMock())

    @patch("empty_bucket.purge_s3_bucket")
    def test_handler_delete_access_denied_failure(self, mock_purge):
        mock_purge.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "Delete")
        event = {"RequestType": "Delete", "ResourceProperties": {"BucketName": "locked-bucket"}}
        with self.assertRaises(ClientError):
            empty_bucket.lambda_handler(event, MagicMock())


if __name__ == "__main__":
    unittest.main()
