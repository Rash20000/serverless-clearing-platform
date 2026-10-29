"""Unit tests verifying shared-infra CloudFormation template compliance."""

import os
import re
import unittest

TEMPLATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "template.yaml"))


class TestSharedInfraTemplate(unittest.TestCase):
    def setUp(self):
        self.assertTrue(os.path.exists(TEMPLATE_PATH), f"template.yaml not found at {TEMPLATE_PATH}")
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            self.template_text = f.read()

    def test_parameters_defined(self):
        self.assertIn("Environment:", self.template_text)
        self.assertIn("EnablePinpoint:", self.template_text)

    def test_archive_bucket_security_and_lifecycle(self):
        self.assertIn("SharedArchiveBucket:", self.template_text)
        self.assertIn("DeletionPolicy: Retain", self.template_text)
        self.assertIn("UpdateReplacePolicy: Retain", self.template_text)
        self.assertIn("SSEAlgorithm: AES256", self.template_text)
        self.assertIn("Status: Enabled", self.template_text)  # Versioning
        self.assertIn("BlockPublicAcls: true", self.template_text)
        self.assertIn("BlockPublicPolicy: true", self.template_text)
        self.assertIn("IgnorePublicAcls: true", self.template_text)
        self.assertIn("RestrictPublicBuckets: true", self.template_text)
        self.assertIn("StorageClass: GLACIER", self.template_text)

    def test_tls_only_bucket_policy(self):
        self.assertIn("SharedArchiveBucketPolicy:", self.template_text)
        self.assertIn("aws:SecureTransport", self.template_text)
        self.assertIn('Effect: Deny', self.template_text)

    def test_custom_empty_bucket_cleanup(self):
        self.assertIn("EmptySharedArchiveBucket:", self.template_text)
        self.assertIn("Type: Custom::EmptyBucket", self.template_text)
        self.assertIn("EmptyBucketServiceToken", self.template_text)

    def test_cache_report_table_spec(self):
        self.assertIn("SharedCacheReportTable:", self.template_text)
        self.assertIn("TableName: !Sub \"${Environment}-cache-report\"", self.template_text)
        self.assertIn("PointInTimeRecoveryEnabled: true", self.template_text)
        self.assertIn("SSEEnabled: true", self.template_text)
        self.assertIn("AttributeName: ttl", self.template_text)

    def test_reference_table_spec(self):
        self.assertIn("SharedReferenceTable:", self.template_text)
        self.assertIn("TableName: !Sub \"${Environment}-reference\"", self.template_text)

    def test_pinpoint_application_defined(self):
        self.assertIn("SharedPinpointApp:", self.template_text)
        self.assertIn("Type: AWS::Pinpoint::App", self.template_text)

    def test_cross_stack_exports(self):
        expected_exports = [
            "${Environment}-SharedArchiveBucketName",
            "${Environment}-SharedArchiveBucketArn",
            "${Environment}-CacheReportTableName",
            "${Environment}-CacheReportTableArn",
            "${Environment}-ReferenceTableName",
            "${Environment}-ReferenceTableArn",
            "${Environment}-SharedPinpointAppId",
            "${Environment}-SharedPinpointAppArn",
        ]
        for exp in expected_exports:
            self.assertIn(exp, self.template_text, f"Missing expected export: {exp}")


if __name__ == "__main__":
    unittest.main()
