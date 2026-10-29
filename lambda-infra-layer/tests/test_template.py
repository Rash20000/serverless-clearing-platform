"""Unit tests verifying lambda-infra-layer SAM template compliance."""

import os
import unittest

TEMPLATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "template.yaml"))


class TestLambdaInfraLayerTemplate(unittest.TestCase):
    def setUp(self):
        self.assertTrue(os.path.exists(TEMPLATE_PATH), f"template.yaml not found at {TEMPLATE_PATH}")
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            self.template_text = f.read()

    def test_sam_transform_and_parameters(self):
        self.assertIn("Transform: AWS::Serverless-2016-10-31", self.template_text)
        self.assertIn("Environment:", self.template_text)

    def test_layers_defined(self):
        self.assertIn("PandasLayer:", self.template_text)
        self.assertIn("OpenPyXLLayer:", self.template_text)
        self.assertIn("CrHelperLayer:", self.template_text)
        self.assertIn("Type: AWS::Serverless::LayerVersion", self.template_text)

    def test_layer_directories_exist(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.assertTrue(os.path.isdir(os.path.join(base_dir, "layers", "pandas", "python")))
        self.assertTrue(os.path.isdir(os.path.join(base_dir, "layers", "openpyxl", "python")))
        self.assertTrue(os.path.isdir(os.path.join(base_dir, "layers", "crhelper", "python")))

    def test_ssm_parameters_defined(self):
        self.assertIn("PandasLayerParameter:", self.template_text)
        self.assertIn("OpenPyXLLayerParameter:", self.template_text)
        self.assertIn("CrHelperLayerParameter:", self.template_text)
        self.assertIn("/${Environment}/lambda-layers/pandas-arn", self.template_text)
        self.assertIn("/${Environment}/lambda-layers/openpyxl-arn", self.template_text)
        self.assertIn("/${Environment}/lambda-layers/crhelper-arn", self.template_text)

    def test_outputs_and_exports_defined(self):
        self.assertIn("PandasLayerArn:", self.template_text)
        self.assertIn("OpenPyXLLayerArn:", self.template_text)
        self.assertIn("CrHelperLayerArn:", self.template_text)
        self.assertIn("${Environment}-PandasLayerArn", self.template_text)
        self.assertIn("${Environment}-OpenPyXLLayerArn", self.template_text)
        self.assertIn("${Environment}-CrHelperLayerArn", self.template_text)


if __name__ == "__main__":
    unittest.main()
