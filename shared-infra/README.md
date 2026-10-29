# Shared Infrastructure Stack (`shared-infra`)

[← Back to Platform Architecture](../README.md)

> **Subsystem Scope**: Foundational AWS storage, DynamoDB persistence tiers, strict TLS 1.3 transport security, and shared layer exports.
> Responsible for account-level persistent storage (S3 archive, DynamoDB cache and reference tables), encryption standards, PITR disaster recovery, and organization-wide configuration exports.

---

## Subsystem Interconnections
- **Downstream Consumers**: Provisions central DynamoDB tables (`cache.report`, `reference`) and S3 archive buckets consumed by [`stream-processing-pipeline`](../stream-processing-pipeline/README.md), [`core-data-api`](../core-data-api/README.md), and [`file-ingestion-pipeline`](../file-ingestion-pipeline/README.md).
- **Automation Helpers**: Leverages `Custom::EmptyBucket` from [`shared-utils`](../shared-utils/README.md) for automated stack teardowns.
- **Base Layers**: Distributes layer ARNs packaged by [`lambda-infra-layer`](../lambda-infra-layer/README.md).

---

## 1. Production Context & Operational Mission

In multi-pipeline clearing architectures, multiple decoupled microservices (batch file ingestion, stream processing, inquiry APIs, reporting engines) must share persistent datastores and lifecycle policies without circular CloudFormation stack dependencies.

### The Operational Challenge
- **Circular Dependencies**: If individual service stacks declare their own tables or buckets and cross-import each other's outputs, stack updates frequently deadlock with circular export errors.
- **Data Deletion Protection**: Operational clearing ledgers and archive reports must be protected against accidental stack deletion (`DeletionPolicy: Retain`), while still supporting automated stack destruction during ephemeral test environment teardowns.
- **Security & Compliance Audits**: Enforcing SOC2 and financial data security standards (TLS 1.3 transport, SSE-AES256 encryption, 365-day archive lifecycles) must be applied centrally, not left to individual development teams.

---

## 2. Engineering Tradeoffs & Architectural Strategy

### A. Centralized Foundation Storage Layer
`shared-infra` decouples persistent state from transient compute:
- **`SharedArchiveBucket`**: Central S3 repository for daily settlement reports and valuation digests:
  - Enforces `aws:SecureTransport: "false"` bucket policy denial (all non-HTTPS traffic is rejected).
  - Configures automated Glacier lifecycle transitions at 90 days and non-current version expiration at 365 days.
  - Integrates with `Custom::EmptyBucket` from `shared-utils` for automated cleanup during stack teardown.
- **`SharedCacheReportTable`**: 36-partition sharded DynamoDB table (`PK = job-x#{suffix}`, `SK = YYYYMMDD#{id}`) for high-throughput reporting queries with Point-in-Time Recovery (PITR) enabled.
- **`SharedReferenceTable`**: Entity master reference data table for low-latency enrichment lookups.

### B. Organization-Wide SSM & Export Resolution
All foundation outputs are published to both CloudFormation Exports and AWS SSM Parameter Store:
- `/${Environment}/shared-infra/PowertoolsLayerArn`
- `/${Environment}/shared-infra/CrHelperLayerArn`

---

## 3. Operational Constraints & Compliance Standards

| Standard / Parameter | Production Configuration | Compliance Justification |
|---|---|---|
| **S3 Transport Security** | Deny all HTTP (`aws:SecureTransport: false`) | Financial regulatory mandate for data-in-transit protection. |
| **Data-at-Rest Encryption** | SSE-AES256 (S3), AWS-owned KMS (DynamoDB) | Protects clearing and valuation records at rest. |
| **Point-in-Time Recovery** | `PointInTimeRecoveryEnabled: true` | Protects against accidental bulk data modification or administrative error. |
| **Archive Lifecycle** | 90 days to Glacier, 365-day expiration | Satisfies financial regulatory document retention requirements while minimizing storage cost. |

---

## 4. Repository Structure

```
shared-infra/
├── layers/
│   └── crhelper/                  # Pre-packaged crhelper layer
├── tests/
│   ├── __init__.py                # Test setup
│   └── test_template.py           # Unit tests verifying template compliance, encryption, and policies
├── template.yaml                  # SAM / CloudFormation foundation storage definition
├── build.yaml                     # AWS CodeBuild CI/CD specification
├── diagram.md                     # Architecture diagrams
└── readme.md                      # Subsystem documentation
```

---

## 5. Local Verification & Unit Testing

```bash
python -m unittest discover tests -v
```
