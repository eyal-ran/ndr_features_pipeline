# NDR Deployment Notebook (Markdown Export)

This file mirrors `canonical_end_to_end_deployment_plan.ipynb` with explicit Markdown/Code blocks for easy copy/paste.

## Cell 1 (Markdown)

# NDR Deployment Notebook (Single Source of Truth)

This notebook is the **authoritative deployment runbook** for the NDR feature pipeline.
It intentionally combines all deployment-related instructions in one place and is designed to be copied into **SageMaker Studio JupyterLab**.

---

## Deployment principles enforced in this notebook
1. **Step Functions** are deployed **manually** in the AWS Step Functions GUI (Build by code).
2. **DynamoDB tables + seed data** are deployed **by code** from this notebook, using pre-defined dictionaries.
3. **SageMaker Pipelines + steps** are deployed **by code** from this notebook, using pre-defined dictionaries.
4. Each deployment section lists **deployment form** (manual GUI vs notebook code) and exact execution details.
5. Includes the required **S3 schema** for ML bucket bootstrapping.
6. Includes initial deployment steps to create **code artifacts** consumed by pipeline steps.
7. Includes additional helper utilities for safer and easier deployment.


## Cell 2 (Markdown)

## 0) Recommended deployment order and forms

| Order | Component | Deployment form | Where |
|---|---|---|---|
| 1 | AWS context + naming variables | By code | This notebook |
| 2 | S3 project schema/prefixes | By code | This notebook |
| 3 | Code artifact manifests + packaging plan | By code | This notebook |
| 4 | DynamoDB tables + initial seed rows | By code | This notebook |
| 5 | SageMaker Pipelines (and their step config) | By code | This notebook |
| 6 | Step Functions state machines | **Manual** | AWS Step Functions GUI (Build by code) |
| 7 | Post-deploy validation checks | By code | This notebook |

> Keep Step Functions for last so they reference deployed pipelines and tables.

## Cell 3 (Code)

```python
# 1) Environment / dependencies (run once)
# If running in a fresh SageMaker JupyterLab kernel, uncomment as needed:
# !pip install -q boto3 sagemaker

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3

try:
    import sagemaker
    from sagemaker.workflow.pipeline import Pipeline
except Exception:
    sagemaker = None
    Pipeline = None

print('UTC now:', datetime.now(timezone.utc).isoformat())
```

## Cell 4 (Markdown)

## 2) Variables (define first, then consume in dictionaries)

> Per requirement, every dictionary below references variables with the same semantic names.
> If a value is unknown at first deployment time, keep it empty and leave an inline placeholder comment.

## Cell 5 (Code)

```python
# 2.1 Core AWS/project variables
aws_region = 'us-east-1'  # e.g., 'us-east-1'
aws_account_id = ''  # e.g., '123456789012'
project_name = 'ndr'
environment_name = 'dev'  # e.g., dev/stage/prod

ml_bucket_name = ''  # e.g., 'ndr-ml-project-dev-us-east-1'
code_artifacts_prefix = 'artifacts/code'
pipeline_artifacts_prefix = 'artifacts/pipeline'
raw_input_prefix = 'input/raw'
processed_prefix = 'output/processed'
feature_store_offline_prefix = 'feature-store/offline'
monitoring_prefix = 'monitoring'
logs_prefix = 'logs'

sagemaker_execution_role_arn = ''  # e.g., 'arn:aws:iam::123456789012:role/service-role/AmazonSageMaker-ExecutionRole-...'
kms_key_arn = ''  # e.g., 'arn:aws:kms:us-east-1:123456789012:key/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'

image_uri = ''  # e.g., '<account>.dkr.ecr.<region>.amazonaws.com/ndr-processing:latest'
source_dir_s3_uri = ''  # e.g., 's3://.../artifacts/code/ndr_code_bundle.tar.gz'
entrypoint_module = 'ndr.scripts.run_delta_builder'
```

## Cell 6 (Code)

```python
# 2.2 DynamoDB table-level variables + initial data variables
# --- Table 1: ml_projects_parameters ---
ml_projects_parameters_table_name = f'{project_name}-{environment_name}-ml-projects-parameters'
ml_projects_parameters_billing_mode = 'PAY_PER_REQUEST'
ml_projects_parameters_hash_key = 'project_name'
ml_projects_parameters_range_key = 'parameter_name'

seed_project_name = project_name
seed_parameter_name = 's3_ml_bucket'
seed_parameter_value = ml_bucket_name  # e.g., 'ndr-ml-project-dev-us-east-1'
seed_parameter_description = 'Primary ML bucket for NDR deployment assets'

# --- Table 2: orchestration_state ---
orchestration_state_table_name = f'{project_name}-{environment_name}-orchestration-state'
orchestration_state_billing_mode = 'PAY_PER_REQUEST'
orchestration_state_hash_key = 'pk'
orchestration_state_range_key = 'sk'

seed_state_pk = f'{project_name}#bootstrap'
seed_state_sk = 'current'
seed_state_payload = {
    'phase': 'initialized',
    'updated_at_utc': datetime.now(timezone.utc).isoformat(),
}

# --- Table 3: artifact_registry ---
artifact_registry_table_name = f'{project_name}-{environment_name}-artifact-registry'
artifact_registry_billing_mode = 'PAY_PER_REQUEST'
artifact_registry_hash_key = 'artifact_name'
artifact_registry_range_key = 'artifact_version'

seed_artifact_name = 'ndr_code_bundle'
seed_artifact_version = 'v1'  # e.g., semantic version or git SHA
seed_artifact_s3_uri = ''  # e.g., 's3://bucket/artifacts/code/ndr_code_bundle.tar.gz'
seed_artifact_created_at_utc = datetime.now(timezone.utc).isoformat()
```

## Cell 7 (Code)

```python
# 2.3 SageMaker pipeline variables
inference_pipeline_name = f'{project_name}-{environment_name}-inference'
training_pipeline_name = f'{project_name}-{environment_name}-training'
backfill_pipeline_name = f'{project_name}-{environment_name}-backfill'

instance_type_processing = 'ml.m5.2xlarge'  # adjust by workload
instance_count_processing = 1
max_runtime_seconds = 7200

pipeline_network_subnet_ids = []  # e.g., ['subnet-aaa', 'subnet-bbb']
pipeline_security_group_ids = []  # e.g., ['sg-aaa111']

# Step-level args placeholders (fill exact values before production rollout)
delta_builder_window_minutes = 15
fg_a_lookback_days = 30
fg_b_lookback_days = 90
fg_c_lookback_days = 90
```

## Cell 8 (Markdown)

## 3) Pre-made dictionaries (table definitions + seed data)

Each table has a single dictionary that includes:
- table metadata (name, keys, billing, schema), and
- initial records to seed immediately after creation.

## Cell 9 (Code)

```python
ddb_tables_deployment_plan: Dict[str, Dict[str, Any]] = {
    'ml_projects_parameters': {
        'table_name': ml_projects_parameters_table_name,
        'billing_mode': ml_projects_parameters_billing_mode,
        'key_schema': [
            {'AttributeName': ml_projects_parameters_hash_key, 'KeyType': 'HASH'},
            {'AttributeName': ml_projects_parameters_range_key, 'KeyType': 'RANGE'},
        ],
        'attribute_definitions': [
            {'AttributeName': ml_projects_parameters_hash_key, 'AttributeType': 'S'},
            {'AttributeName': ml_projects_parameters_range_key, 'AttributeType': 'S'},
        ],
        'seed_items': [
            {
                'project_name': seed_project_name,
                'parameter_name': seed_parameter_name,
                'parameter_value': seed_parameter_value,
                'description': seed_parameter_description,
            }
        ],
    },
    'orchestration_state': {
        'table_name': orchestration_state_table_name,
        'billing_mode': orchestration_state_billing_mode,
        'key_schema': [
            {'AttributeName': orchestration_state_hash_key, 'KeyType': 'HASH'},
            {'AttributeName': orchestration_state_range_key, 'KeyType': 'RANGE'},
        ],
        'attribute_definitions': [
            {'AttributeName': orchestration_state_hash_key, 'AttributeType': 'S'},
            {'AttributeName': orchestration_state_range_key, 'AttributeType': 'S'},
        ],
        'seed_items': [
            {
                'pk': seed_state_pk,
                'sk': seed_state_sk,
                'payload': seed_state_payload,
            }
        ],
    },
    'artifact_registry': {
        'table_name': artifact_registry_table_name,
        'billing_mode': artifact_registry_billing_mode,
        'key_schema': [
            {'AttributeName': artifact_registry_hash_key, 'KeyType': 'HASH'},
            {'AttributeName': artifact_registry_range_key, 'KeyType': 'RANGE'},
        ],
        'attribute_definitions': [
            {'AttributeName': artifact_registry_hash_key, 'AttributeType': 'S'},
            {'AttributeName': artifact_registry_range_key, 'AttributeType': 'S'},
        ],
        'seed_items': [
            {
                'artifact_name': seed_artifact_name,
                'artifact_version': seed_artifact_version,
                's3_uri': seed_artifact_s3_uri,
                'created_at_utc': seed_artifact_created_at_utc,
            }
        ],
    },
}

print(json.dumps(ddb_tables_deployment_plan, indent=2, default=str))
```

## Cell 10 (Code)

```python
# DynamoDB deploy helpers (safe/idempotent)
def ensure_ddb_table(ddb_client, spec: Dict[str, Any], dry_run: bool = True) -> None:
    table_name = spec['table_name']
    if dry_run:
        print(f'[DRY RUN] Would ensure table exists: {table_name}')
        return

    existing = ddb_client.list_tables()['TableNames']
    if table_name not in existing:
        print(f'Creating table: {table_name}')
        ddb_client.create_table(
            TableName=table_name,
            BillingMode=spec['billing_mode'],
            KeySchema=spec['key_schema'],
            AttributeDefinitions=spec['attribute_definitions'],
        )
        waiter = ddb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
    else:
        print(f'Table already exists: {table_name}')


def seed_ddb_items(ddb_resource, spec: Dict[str, Any], dry_run: bool = True) -> None:
    table_name = spec['table_name']
    items = spec.get('seed_items', [])
    if dry_run:
        print(f'[DRY RUN] Would seed {len(items)} item(s) into {table_name}')
        return

    table = ddb_resource.Table(table_name)
    for item in items:
        table.put_item(Item=item)
    print(f'Seeded {len(items)} item(s) into {table_name}')


def deploy_all_ddb_tables(plan: Dict[str, Dict[str, Any]], region_name: str, dry_run: bool = True) -> None:
    ddb_client = boto3.client('dynamodb', region_name=region_name)
    ddb_resource = boto3.resource('dynamodb', region_name=region_name)

    for table_id, table_spec in plan.items():
        print(f'--- Deploying DDB table: {table_id} ---')
        ensure_ddb_table(ddb_client, table_spec, dry_run=dry_run)
        seed_ddb_items(ddb_resource, table_spec, dry_run=dry_run)

print('DynamoDB helpers ready.')
```

## Cell 11 (Markdown)

## 4) S3 schema creation (ML project bucket)

This creates/validates required S3 prefixes for raw input, outputs, artifacts, and monitoring.

## Cell 12 (Code)

```python
s3_schema_plan = {
    'bucket_name': ml_bucket_name,
    'prefixes': [
        raw_input_prefix,
        f'{processed_prefix}/delta_15m',
        f'{processed_prefix}/fg_a',
        f'{processed_prefix}/fg_b',
        f'{processed_prefix}/fg_c',
        f'{processed_prefix}/prediction_join',
        code_artifacts_prefix,
        pipeline_artifacts_prefix,
        feature_store_offline_prefix,
        monitoring_prefix,
        logs_prefix,
    ],
}


def ensure_s3_schema(schema: Dict[str, Any], region_name: str, dry_run: bool = True) -> None:
    s3 = boto3.client('s3', region_name=region_name)
    bucket = schema['bucket_name']

    if dry_run:
        print(f'[DRY RUN] Validate/create bucket: {bucket}')
        for p in schema['prefixes']:
            print(f'[DRY RUN] Ensure prefix: s3://{bucket}/{p}/')
        return

    if not bucket:
        raise ValueError('bucket_name must be set before non-dry-run execution')

    existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if bucket not in existing_buckets:
        params = {'Bucket': bucket}
        if region_name != 'us-east-1':
            params['CreateBucketConfiguration'] = {'LocationConstraint': region_name}
        s3.create_bucket(**params)
        print(f'Created bucket: {bucket}')

    for p in schema['prefixes']:
        s3.put_object(Bucket=bucket, Key=f"{p.rstrip('/')}/")
        print(f"Ensured prefix: s3://{bucket}/{p.rstrip('/')}/")

print(json.dumps(s3_schema_plan, indent=2))
```

## Cell 13 (Markdown)

## 5) Initial code artifact creation (pipeline-consumed artifacts)

This section unifies artifact preparation instructions and notebook-executable commands.

### What pipeline steps consume
- Source bundle tarball (Python package + scripts).
- Optional dependency manifest.
- Build metadata (version, git SHA, build timestamp, source URI).

## Cell 14 (Code)

```python
# Artifact dictionary + helper variables
code_bundle_local_path = '/tmp/ndr_code_bundle.tar.gz'
code_bundle_s3_uri = f"s3://{ml_bucket_name}/{code_artifacts_prefix}/ndr_code_bundle.tar.gz" if ml_bucket_name else ''
requirements_local_path = '/tmp/requirements.txt'
artifact_version = seed_artifact_version
artifact_git_sha = ''  # e.g., output of `git rev-parse --short HEAD`
artifact_created_at = datetime.now(timezone.utc).isoformat()

code_artifact_plan = {
    'artifact_name': seed_artifact_name,
    'artifact_version': artifact_version,
    'artifact_git_sha': artifact_git_sha,
    'created_at_utc': artifact_created_at,
    'bundle_local_path': code_bundle_local_path,
    'bundle_s3_uri': code_bundle_s3_uri,
    'requirements_local_path': requirements_local_path,
}

print(json.dumps(code_artifact_plan, indent=2))
```

## Cell 15 (Code)

```python
# Notebook-executable artifact creation instructions
# Run from repository root in SageMaker JupyterLab terminal/cell magic.

artifact_shell_steps = [
    "python -m ndr.scripts.run_code_bundle_build --output /tmp/ndr_code_bundle.tar.gz",
    "python -m ndr.scripts.run_code_smoke_validate --bundle /tmp/ndr_code_bundle.tar.gz",
]

for i, step in enumerate(artifact_shell_steps, start=1):
    print(f'{i}. {step}')


def upload_artifact_to_s3(local_path: str, s3_uri: str, region_name: str, dry_run: bool = True) -> None:
    if dry_run:
        print(f'[DRY RUN] Would upload {local_path} -> {s3_uri}')
        return
    if not s3_uri.startswith('s3://'):
        raise ValueError('s3_uri must start with s3://')
    _, _, bucket_and_key = s3_uri.partition('s3://')
    bucket, _, key = bucket_and_key.partition('/')
    boto3.client('s3', region_name=region_name).upload_file(local_path, bucket, key)
    print(f'Uploaded {local_path} -> {s3_uri}')
```

## Cell 16 (Markdown)

## 6) SageMaker Pipelines deployment (dictionary-driven)

Define each pipeline in a dictionary that captures its runtime contract. Then call deployment helper.

## Cell 17 (Code)

```python
sagemaker_pipelines_plan: Dict[str, Dict[str, Any]] = {
    'inference': {
        'pipeline_name': inference_pipeline_name,
        'role_arn': sagemaker_execution_role_arn,
        'image_uri': image_uri,
        'source_dir_s3_uri': source_dir_s3_uri,
        'steps': [
            {'name': 'delta_builder', 'entrypoint_module': 'ndr.scripts.run_delta_builder', 'window_minutes': delta_builder_window_minutes},
            {'name': 'fg_a_builder', 'entrypoint_module': 'ndr.scripts.run_fg_a_builder', 'lookback_days': fg_a_lookback_days},
            {'name': 'fg_c_builder', 'entrypoint_module': 'ndr.scripts.run_fg_c_builder', 'lookback_days': fg_c_lookback_days},
            {'name': 'prediction_feature_join', 'entrypoint_module': 'ndr.scripts.run_prediction_feature_join'},
            {'name': 'inference_predictions', 'entrypoint_module': 'ndr.scripts.run_inference_predictions'},
        ],
        'processing_resources': {
            'instance_type': instance_type_processing,
            'instance_count': instance_count_processing,
            'max_runtime_seconds': max_runtime_seconds,
        },
    },
    'training': {
        'pipeline_name': training_pipeline_name,
        'role_arn': sagemaker_execution_role_arn,
        'image_uri': image_uri,
        'source_dir_s3_uri': source_dir_s3_uri,
        'steps': [
            {'name': 'delta_builder', 'entrypoint_module': 'ndr.scripts.run_delta_builder', 'window_minutes': delta_builder_window_minutes},
            {'name': 'fg_a_builder', 'entrypoint_module': 'ndr.scripts.run_fg_a_builder', 'lookback_days': fg_a_lookback_days},
            {'name': 'fg_b_builder', 'entrypoint_module': 'ndr.scripts.run_fg_b_builder', 'lookback_days': fg_b_lookback_days},
            {'name': 'if_training', 'entrypoint_module': 'ndr.scripts.run_if_training'},
        ],
        'processing_resources': {
            'instance_type': instance_type_processing,
            'instance_count': instance_count_processing,
            'max_runtime_seconds': max_runtime_seconds,
        },
    },
    'backfill': {
        'pipeline_name': backfill_pipeline_name,
        'role_arn': sagemaker_execution_role_arn,
        'image_uri': image_uri,
        'source_dir_s3_uri': source_dir_s3_uri,
        'steps': [
            {'name': 'historical_windows_extractor', 'entrypoint_module': 'ndr.scripts.run_historical_windows_extractor'},
            {'name': 'backfill_reprocessing', 'entrypoint_module': 'ndr.scripts.run_backfill_reprocessing_executor'},
        ],
        'processing_resources': {
            'instance_type': instance_type_processing,
            'instance_count': instance_count_processing,
            'max_runtime_seconds': max_runtime_seconds,
        },
    },
}

print(json.dumps(sagemaker_pipelines_plan, indent=2))
```

## Cell 18 (Code)

```python
# Generic pipeline deploy helper.
# You can either:
# (A) call your existing project pipeline builder functions and upsert, or
# (B) export the dictionary to your internal deployment wrapper.

def deploy_sagemaker_pipelines(plan: Dict[str, Dict[str, Any]], region_name: str, dry_run: bool = True) -> None:
    if dry_run:
        for pipeline_id, spec in plan.items():
            print(f"[DRY RUN] Would deploy pipeline '{pipeline_id}' as {spec['pipeline_name']} with {len(spec['steps'])} step(s).")
        return

    if sagemaker is None:
        raise RuntimeError('sagemaker SDK is not installed in this kernel')

    # Stub integration point: replace with concrete build functions from src/ndr/pipeline/*.py
    for pipeline_id, spec in plan.items():
        print(f"Deploying pipeline '{pipeline_id}' -> {spec['pipeline_name']}")
        # Example (pseudo): pipeline_obj = build_pipeline_from_spec(spec, region_name)
        # pipeline_obj.upsert(role_arn=spec['role_arn'])
        # pipeline_obj.start()
        print(f"Pipeline upsert placeholder completed for {spec['pipeline_name']}")
```

## Cell 19 (Markdown)

## 7) Step Functions deployment (MANUAL via AWS GUI)

Deployment form: **Manual (AWS Step Functions console)**

For each state machine JSON definition under `docs/step_functions_jsonata/`:
1. Open AWS Console → Step Functions → **Create state machine** (or update existing).
2. Choose **Author with code snippets / Build by code**.
3. Copy-paste the JSON definition content.
4. Set execution role ARN and logging configuration.
5. Save and publish new revision.

Recommended deployment order:
1. `sfn_ndr_code_deployment_orchestrator.json`
2. `sfn_ndr_initial_deployment_bootstrap.json`
3. `sfn_ndr_15m_features_inference.json`
4. `sfn_ndr_training_orchestrator.json`
5. `sfn_ndr_monthly_fg_b_baselines.json`
6. `sfn_ndr_backfill_reprocessing.json`
7. `sfn_ndr_prediction_publication.json`

> Keep a deployment log table with state machine ARN + revision + deployment timestamp.

## Cell 20 (Markdown)

## 8) Execute deployment (recommended run blocks)

### 8.1 Dry run (mandatory)
Run this first to validate dictionaries and flow without making cloud changes.

## Cell 21 (Code)

```python
DRY_RUN = True

deploy_all_ddb_tables(ddb_tables_deployment_plan, region_name=aws_region, dry_run=DRY_RUN)
ensure_s3_schema(s3_schema_plan, region_name=aws_region, dry_run=DRY_RUN)
upload_artifact_to_s3(code_bundle_local_path, code_bundle_s3_uri, region_name=aws_region, dry_run=DRY_RUN)
deploy_sagemaker_pipelines(sagemaker_pipelines_plan, region_name=aws_region, dry_run=DRY_RUN)

print('Dry run complete.')
```

## Cell 22 (Markdown)

### 8.2 Actual deployment
After all variables are filled and dry run is clean:
- set `DRY_RUN = False`
- re-run the deployment cell.

## Cell 23 (Markdown)

## 9) Post-deployment validation checks

Run these checks to confirm all resources exist and were initialized.

## Cell 24 (Code)

```python
def check_ddb_tables_exist(plan: Dict[str, Dict[str, Any]], region_name: str) -> List[str]:
    ddb = boto3.client('dynamodb', region_name=region_name)
    existing = set(ddb.list_tables()['TableNames'])
    missing = []
    for spec in plan.values():
        if spec['table_name'] not in existing:
            missing.append(spec['table_name'])
    return missing


def check_s3_prefixes_exist(schema: Dict[str, Any], region_name: str) -> List[str]:
    s3 = boto3.client('s3', region_name=region_name)
    bucket = schema['bucket_name']
    missing = []
    for prefix in schema['prefixes']:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix.rstrip('/')}/", MaxKeys=1)
        if resp.get('KeyCount', 0) == 0:
            missing.append(prefix)
    return missing

print('Validation helpers ready.')
```

## Cell 25 (Code)

```python
# Offline-safe structural validations (can be run locally before cloud deployment)
assert 'ml_projects_parameters' in ddb_tables_deployment_plan
assert all('table_name' in spec for spec in ddb_tables_deployment_plan.values())
assert all('seed_items' in spec for spec in ddb_tables_deployment_plan.values())
assert all('pipeline_name' in spec for spec in sagemaker_pipelines_plan.values())
assert len(s3_schema_plan['prefixes']) > 0

print('Local structural validations passed.')
```

## Cell 26 (Markdown)

## 10) Operational recommendations (optional but strongly recommended)

- Enable CloudWatch log groups with retention policy for pipeline processing jobs and state machines.
- Add EventBridge rules for pipeline failure notifications to SNS/Slack.
- Version-lock code bundles by git SHA and preserve immutable artifacts.
- Keep a deployment changelog itemizing: what changed, who deployed, when, and rollback plan.
