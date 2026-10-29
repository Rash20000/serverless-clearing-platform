# Lambda Infrastructure Layers (`lambda-infra-layer`)

[← Back to Platform Architecture](../README.md)

> **Subsystem Scope**: Centralized build, packaging, and publishing automation for cross-subsystem Lambda runtime dependency layers.
> Responsible for cross-service binary compilation (`pandas`, `openpyxl`, `crhelper`), automated CodeBuild artifact generation, and zero-drift SSM layer distribution.

---

## Subsystem Interconnections
- **Consuming Services**: Distributes runtime packages to [`stream-processing-pipeline`](../stream-processing-pipeline/README.md) (analytical Pandas reports), [`shared-utils`](../shared-utils/README.md) (`crhelper` custom resources), and [`core-data-api`](../core-data-api/README.md).
- **Deployment Precedence**: First stack in the topological deployment order (must be deployed before stacks declaring layer dependencies).

---

## 1. Layers Managed

| Layer Name | Target Runtimes | Architecture | Contents | Description |
|---|---|---|---|---|
| **`PandasLayer`** | `python3.12` | `arm64` | `pandas`, `numpy`, `python-dateutil`, `pytz`, `tzdata` | Analytical queries, dataframe manipulation, tabular aggregations. |
| **`OpenPyXLLayer`** | `python3.12` | `arm64` | `openpyxl`, `et_xmlfile` | High-performance Excel (`.xlsx`) generation and document reporting. |
| **`CrHelperLayer`** | `python3.12` | `arm64` | `crhelper` | AWS CloudFormation Custom Resource Helper framework for lifecycle events. |

---

## 2. CI/CD & Build Strategy

Dependency packages are **never committed to Git**. The repository tracks only package specifications (`requirements.txt`), SAM infrastructure, and test suites.

- **Automated CI/CD**: AWS CodeBuild (`build.yaml`) compiles and downloads target-platform wheels (`manylinux2014_aarch64` / Python 3.12) into `layers/<layer>/python` during the build phase before executing `sam deploy`.
- **Git Ignore**: All compiled wheels and packages under `layers/**/python/*` are strictly ignored via `.gitignore`.

---

## 3. Local Installation & Development Guide

If you need to install packages locally to inspect code, test changes, or invoke functions locally via SAM CLI, follow the steps below.

### Prerequisites
- Python 3.12+
- `pip`

### A. Installing All Layers Locally (ARM64 Lambda Target)
To package dependencies exactly matching the AWS Lambda execution environment (ARM64 / Linux):

```bash
# 1. Pandas & NumPy layer
pip install --only-binary=:all: --platform manylinux2014_aarch64 --implementation cp --python-version 312 -r layers/pandas/requirements.txt -t layers/pandas/python

# 2. OpenPyXL layer
pip install --only-binary=:all: --platform manylinux2014_aarch64 --implementation cp --python-version 312 -r layers/openpyxl/requirements.txt -t layers/openpyxl/python

# 3. CrHelper layer
pip install -r layers/crhelper/requirements.txt -t layers/crhelper/python
```

### B. Installing for Local Development & IDE Intellisense (Host OS)
If you are developing or running local scripts on Windows/macOS and want packages installed directly into your local virtual environment:

```bash
# In your local virtualenv
pip install -r layers/pandas/requirements.txt
pip install -r layers/openpyxl/requirements.txt
pip install -r layers/crhelper/requirements.txt
```

### C. Cleaning Local Build Artifacts
To wipe local downloaded packages before a fresh build or commit:
```bash
# PowerShell
Remove-Item -Path "layers\*\python\*" -Exclude ".gitkeep" -Recurse -Force
```

---

## 4. Parameter Store & CloudFormation Resolution

Every layer publishes its deployed ARN to both AWS Systems Manager (SSM) Parameter Store and CloudFormation Exports:

### Systems Manager Parameter Store Paths
- **Pandas**: `/${Environment}/lambda-layers/pandas-arn`
- **OpenPyXL**: `/${Environment}/lambda-layers/openpyxl-arn`
- **CrHelper**: `/${Environment}/lambda-layers/crhelper-arn`

### CloudFormation Export Names
- **Pandas**: `${Environment}-PandasLayerArn`
- **OpenPyXL**: `${Environment}-OpenPyXLLayerArn`
- **CrHelper**: `${Environment}-CrHelperLayerArn`

---

## 5. How to Consume in SAM Templates

### Option A: Via SSM Dynamic Resolution (Recommended for loose coupling)
```yaml
Resources:
  DataAnalyticsFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: analytics_service.lambda_handler
      Layers:
        - '{{resolve:ssm:/dev/lambda-layers/pandas-arn}}'
        - '{{resolve:ssm:/dev/lambda-layers/openpyxl-arn}}'
```

### Option B: Via CloudFormation `Fn::ImportValue`
```yaml
Resources:
  CustomResourceFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: custom_resource.lambda_handler
      Layers:
        - !ImportValue !Sub "${Environment}-CrHelperLayerArn"
```

---

## 6. Repository Structure

```
lambda-infra-layer/
├── layers/
│   ├── pandas/
│   │   ├── requirements.txt       # pandas, numpy, pytz specifications
│   │   └── python/.gitkeep        # Target directory populated at build time
│   ├── openpyxl/
│   │   ├── requirements.txt       # openpyxl, et_xmlfile specifications
│   │   └── python/.gitkeep        # Target directory populated at build time
│   └── crhelper/
│       ├── requirements.txt       # crhelper specification
│       └── python/.gitkeep        # Target directory populated at build time
├── tests/
│   ├── __init__.py
│   └── test_template.py           # SAM template & parameter tests
├── build.yaml                     # AWS CodeBuild spec that downloads & packages layers
├── diagram.md                     # Architecture & layer consumption flow
├── template.yaml                  # AWS SAM infrastructure definition
├── .gitignore                     # Ignores layers/**/python/* artifacts
└── readme.md                      # Developer instructions & usage guide
```

---

## 7. Running Tests

Run the repository unit tests locally with standard library `unittest`:
```bash
python -m unittest discover tests -v
```
