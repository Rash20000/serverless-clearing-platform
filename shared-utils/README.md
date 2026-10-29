# Shared CloudFormation Custom Utilities (`shared-utils`)

[← Back to Platform Architecture](../README.md)

> **Subsystem Scope**: Reusable CloudFormation custom resource providers powered by crhelper for dynamic stream discovery and resource lifecycle cleanup.
> Responsible for dynamic DynamoDB Stream discovery at deploy time and automated S3 bucket version purging during stack teardowns.

---

## Subsystem Interconnections
- **Consumers**: Imported by [`stream-processing-pipeline`](../stream-processing-pipeline/README.md) for dynamic stream ARN resolution and [`shared-infra`](../shared-infra/README.md) for automated bucket teardown.
- **Runtime Layer**: Runs on the pre-packaged `crhelper` layer provided by [`lambda-infra-layer`](../lambda-infra-layer/README.md).

---

## 1. Production Context & Operational Mission

Enterprise AWS CloudFormation stacks managing event streams and versioned S3 buckets suffer from two well-known production operational failures:
1. **Broken Stream EventSourceMappings**: When a DynamoDB table with Streams enabled is recreated (e.g. key schema update, blue/green recreate), AWS creates a new unique Stream ARN with a timestamp (`.../stream/YYYY-MM-DDTHH:MM:SS.sss`). Downstream CloudFormation stacks importing the static ARN fail immediately.
2. **Failed Stack Deletion Due to Non-Empty Buckets**: CloudFormation fails to delete stacks containing versioned S3 buckets with the error: `"The bucket you are attempting to delete is not empty"`. Ephemeral CI/CD and pull-request environments fail to tear down, causing resource leaks and cost waste.

---

## 2. Engineering Tradeoffs & Architectural Strategy

### A. Dynamic Stream ARN Discovery (`Custom::StreamArnFetcher`)
- Built using the official CloudFormation Custom Resource Helper (`crhelper`).
- Intercepts stack deployment lifecycle events (`@helper.create`, `@helper.update`).
- Calls `dynamodb:DescribeTable` to resolve `Table.LatestStreamArn` in real-time.
- Downstream stacks (e.g. `stream-processing-pipeline`) invoke this custom resource dynamically:
  ```yaml
  SourceStreamArnLookup:
    Type: Custom::StreamArnFetcher
    Properties:
      ServiceToken: !ImportValue !Sub '${Environment}-StreamArnFetcherServiceToken'
      TableName: !Ref SourceTableName

  LoadFunction:
    Type: AWS::Serverless::Function
    Properties:
      Events:
        StreamTrigger:
          Type: DynamoDB
          Properties:
            Stream: !GetAtt SourceStreamArnLookup.StreamArn
  ```

### B. Automated Bucket Version Purging (`Custom::EmptyBucket`)
- Catches the stack `@helper.delete` lifecycle event.
- Executes `bucket.object_versions.delete()` and `bucket.objects.all().delete()` to purge all object versions, delete markers, and unversioned objects.
- Handles `NoSuchBucket` (404) errors gracefully.
- Enables automated, touchless CloudFormation stack deletion across all CI/CD pipelines.

---

## 3. Operational Constraints & Standards

| Parameter / Rule | Production Target | Design Justification |
|---|---|---|
| **Framework Standard** | `from crhelper import CfnResource` | Replaces fragile manual `urllib3` PUT scripts with the official AWS-supported lifecycle framework. |
| **Timeout Ceiling** | `300 seconds` for EmptyBucket | Allows purging large buckets containing hundreds of thousands of versions without timing out. |
| **Export Resolution** | Direct `Fn::ImportValue` | Exposes service tokens cleanly without conditional `!If` parameter fallbacks. |

---

## 4. Repository Structure

```
shared-utils/
├── src/
│   ├── stream_arn_fetcher.py      # Dynamic LatestStreamArn discovery custom resource
│   ├── empty_bucket.py            # S3 bucket version purging custom resource
│   ├── requirements.txt           # Runtime dependencies
│   └── utils.py                   # S3 and DynamoDB client getters, logging
├── tests/
│   ├── __init__.py                # Test setup & crhelper mocking
│   ├── test_stream_arn_fetcher.py # Tests for create/update/delete events and DescribeTable handling
│   ├── test_empty_bucket.py       # Tests for bucket purging, version deletion, NoSuchBucket handling
│   └── test_utils.py              # Tests for client caching and response helpers
├── template.yaml                  # SAM infrastructure (Custom Resource Lambdas, Alarms)
├── build.yaml                     # AWS CodeBuild CI/CD specification
├── diagram.md                     # Sequence diagrams
└── readme.md                      # Subsystem documentation
```

---

## 5. Local Verification & Unit Testing

```bash
python -m unittest discover tests -v
```
