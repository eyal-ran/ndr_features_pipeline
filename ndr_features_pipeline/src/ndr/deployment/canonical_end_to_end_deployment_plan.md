# Canonical End-to-End Deployment Notebook (Markdown Mirror)

This file mirrors `canonical_end_to_end_deployment_plan.ipynb` exactly, with explicit cell boundaries for copy/paste.

Deterministic sync process: update the `.ipynb` first, then regenerate this mirror from notebook JSON cell order with fenced blocks per cell.

Compatibility note: Step Functions permissions/wiring still require `MonthlyStateMachineArn`, `states:StartExecution`, `states:DescribeExecution`, `states:StopExecution`, and least-privilege `sagemaker:StartPipelineExecution` / `sagemaker:DescribePipelineExecution` access for the exact readiness and FG-B pipeline ARNs invoked by orchestration.

## Cell 1 (markdown)

```markdown
# Canonical End-to-End Deployment Notebook (Repaired - conflicts free)

This notebook is rebuilt from code/config sources in `src/ndr` and `tests` only, and provides an executable deployment workflow.
```

## Cell 2 (markdown)

```markdown
## 0) Deployment order and forms

1. Populate environment-specific variables and externally managed IAM role ARNs.
2. Build DynamoDB seed dictionaries and run blocking preflight validation.
3. Reconcile DynamoDB tables/seeds, S3 schema, and the EventBridge bus.
4. Execute code-bundle build, artifact validation, and smoke validation; promote the validated JobSpecs.
5. Upsert all SageMaker Pipelines from the current repository definitions.
6. Reconcile Step Functions and EventBridge schedules from deterministic environment-derived names.
7. Start or safely retry bootstrap materialization and wait for success.
8. Run final structural checks and persist the deterministic deployment marker.
```

## Cell 3 (markdown)

```markdown
### Code Cell 1 Guide

**Behavior:** Import deployment dependencies and expose UTC execution timestamp used for deterministic audit fields.

**Purpose:** Load all pipeline builders/contracts used by later deployment orchestration cells.

**Required input:** Python environment with `ndr` package importable plus notebook kernel access.

**Expected output:** Printed UTC timestamp and initialized module symbols.

**Expected result:** Notebook runtime is ready for DDB-first contract planning and pipeline upsert steps.

**Usage instructions:** Run once per notebook execution before editing deployment variables.

**Runnable example:** `UTC_NOW`
```

## Cell 4 (code)

```python
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

import boto3
from botocore.exceptions import ClientError

from ndr.contracts import DPP_CODE_STEP_KEYS, MLP_CODE_STEP_KEYS
from ndr.config.project_parameters_loader import DEFAULT_DPP_CONFIG_TABLE_NAME, DEFAULT_MLP_CONFIG_TABLE_NAME, DEFAULT_BATCH_INDEX_TABLE_NAME
from ndr.config.exception_table_contracts import EXCEPTION_TABLE_CONTRACTS
from ndr.scripts import create_ml_projects_parameters_table as create_cfg
from ndr.pipeline.sagemaker_pipeline_definitions_unified_with_fgc import build_15m_streaming_pipeline, build_15m_dependent_pipeline, build_fg_b_baseline_pipeline, build_machine_inventory_unload_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_inference import build_inference_predictions_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_prediction_feature_join import build_prediction_feature_join_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_if_training import build_if_training_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_backfill_historical_extractor import build_backfill_historical_extractor_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_backfill_15m_reprocessing import build_backfill_15m_reprocessing_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_code_bundle_build import build_code_bundle_build_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_code_artifact_validate import build_code_artifact_validate_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_code_smoke_validate import build_code_smoke_validate_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_rt_readiness import build_rt_readiness_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_monthly_readiness import build_monthly_readiness_pipeline

UTC_NOW = datetime.now(timezone.utc).isoformat()
print('UTC now:', UTC_NOW)
```

## Cell 5 (markdown)

```markdown
### Code Cell 2 Guide

**Behavior:** Build the canonical pipeline-builder registry and expose runtime-parameter contracts.

**Purpose:** Define producer/consumer contract lookup used by deployment upsert and readiness checks.

**Required input:** Imports from the prior cell must have executed successfully.

**Expected output:** Counts for builders/runtime contracts and exception table defaults.

**Expected result:** Operator can verify expected pipeline families are present before proceeding.

**Usage instructions:** Run immediately after imports; stop if expected builder count is incorrect.

**Runnable example:** `len(PIPELINE_BUILDERS), len(RUNTIME_PARAM_CONTRACTS)`
```

## Cell 6 (code)

```python
PIPELINE_BUILDERS = {
    'pipeline_15m_streaming': build_15m_streaming_pipeline,
    'pipeline_15m_dependent': build_15m_dependent_pipeline,
    'pipeline_fg_b_baseline': build_fg_b_baseline_pipeline,
    'pipeline_machine_inventory_unload': build_machine_inventory_unload_pipeline,
    'pipeline_inference_predictions': build_inference_predictions_pipeline,
    'pipeline_prediction_feature_join': build_prediction_feature_join_pipeline,
    'pipeline_if_training': build_if_training_pipeline,
    'pipeline_backfill_historical_extractor': build_backfill_historical_extractor_pipeline,
    'pipeline_backfill_15m_reprocessing': build_backfill_15m_reprocessing_pipeline,
    'pipeline_code_bundle_build': build_code_bundle_build_pipeline,
    'pipeline_code_artifact_validate': build_code_artifact_validate_pipeline,
    'pipeline_code_smoke_validate': build_code_smoke_validate_pipeline,
    'pipeline_rt_readiness': build_rt_readiness_pipeline,
    'pipeline_monthly_readiness': build_monthly_readiness_pipeline,
}
RUNTIME_PARAM_CONTRACTS = create_cfg.PIPELINE_RUNTIME_PARAMS
print('Pipeline builders:', len(PIPELINE_BUILDERS))
print('Runtime param contracts:', len(RUNTIME_PARAM_CONTRACTS))
print('Exception tables:', {k: v.default_table_name for k, v in EXCEPTION_TABLE_CONTRACTS.items()})
```

## Cell 7 (markdown)

```markdown
### Code Cell 3 Guide

**Behavior:** Declare required deployment inputs, artifact metadata, and contract identifiers.

**Purpose:** Collect all operator-managed runtime parameters in one auditable location.

**Required input:** Concrete values for account, role, bucket, project/spec, artifact identifiers, and the manually incrementable `deployment_revision`.

**Expected output:** Variables are populated in memory for downstream contract generation.

**Expected result:** Notebook fails fast later if placeholder/empty values violate contract checks.

**Usage instructions:** Fill every placeholder before switching `DRY_RUN` to `False`.

**Runnable example:** `project_name, feature_spec_version, artifact_build_id`
```

## Cell 8 (code)

```python
aws_region = 'us-east-1'
aws_account_id = ''  # e.g., 123456789012
project_name = 'ndr'
feature_spec_version = 'v3'
environment_name = 'dev'
deployment_revision = 1  # Increment manually to force a new deployment fingerprint.
owner = ''  # e.g., ml-platform-team

ml_project_name = ''  # e.g., ndr-ml-anomaly
ml_project_names = [ml_project_name] if ml_project_name else []

sagemaker_role_arn = ''  # e.g., arn:aws:iam::...:role/...
step_functions_role_arn = ''  # existing IAM role assumed by Step Functions
eventbridge_invoke_role_arn = ''  # existing IAM role allowing EventBridge to start Step Functions
default_bucket = ''  # e.g., ndr-ml-project-dev-us-east-1
processing_image_uri = ''  # e.g., <acct>.dkr.ecr.<region>.amazonaws.com/ndr-pyspark:latest

org1 = ''  # e.g., tenant-a
org2 = ''  # e.g., site-01
ingestion_prefix = ''  # e.g., s3://bucket/input/raw/tenant-a/site-01/

batch_id = ''  # e.g., 2026-04-11T10:15:00Z__run_1
date_partition = ''  # e.g., 2026/04/11
hour = ''  # e.g., 10
within_hour_run_number = ''  # e.g., 1
etl_ts = ''  # e.g., 2026-04-11T10:15:00Z
raw_parsed_logs_s3_prefix = ''  # e.g., s3://bucket/raw/traffic/org1/org2/2026/04/11/10/1/
reference_month = ''  # e.g., 2026-04
inference_endpoint_name = ''  # existing SageMaker endpoint name
machine_inventory_cluster_identifier = ''
machine_inventory_database = ''
machine_inventory_secret_arn = ''
machine_inventory_iam_role = ''
machine_inventory_source_table = ''

dpp_config_table_name = DEFAULT_DPP_CONFIG_TABLE_NAME
mlp_config_table_name = DEFAULT_MLP_CONFIG_TABLE_NAME
batch_index_table_name = DEFAULT_BATCH_INDEX_TABLE_NAME
routing_table_name = EXCEPTION_TABLE_CONTRACTS['routing'].default_table_name
processing_lock_table_name = EXCEPTION_TABLE_CONTRACTS['processing_lock'].default_table_name
publication_lock_table_name = EXCEPTION_TABLE_CONTRACTS['publication_lock'].default_table_name

artifact_build_id = ''  # e.g., build-20260411-01
artifact_sha256 = ''  # e.g., sha256
artifact_format = 'tar.gz'
code_artifact_s3_uri = ''  # e.g., s3://bucket/artifacts/code/build-.../source.tar.gz
```

## Cell 9 (markdown)

```markdown
### Code Cell 4 Guide

**Behavior:** Declare canonical DPP/MLP S3 roots consumed by DDB config records.

**Purpose:** Ensure all storage contracts are DDB-first and deterministic.

**Required input:** Valid S3 URI roots for each declared output family.

**Expected output:** Root variables for delta/features/prediction/training outputs.

**Expected result:** Seed items can reference stable roots without hidden fallback paths.

**Usage instructions:** Set non-empty roots before seeding production tables.

**Runnable example:** `dpp_delta_root, mlp_predictions_root`
```

## Cell 10 (code)

```python
project_data_root = f's3://{default_bucket}/projects/{project_name}/versions/{feature_spec_version}/data'
dpp_delta_root = f'{project_data_root}/delta/'
dpp_pair_counts_root = f'{project_data_root}/pair_counts/'
dpp_fg_a_root = f'{project_data_root}/fg_a/'
dpp_fg_b_root = f'{project_data_root}/fg_b/'
dpp_fg_c_root = f'{project_data_root}/fg_c/'
dpp_machine_inventory_root = f'{project_data_root}/machine_inventory/'

mlp_predictions_root = f'{project_data_root}/predictions/'
mlp_prediction_join_root = f'{project_data_root}/prediction_feature_join/'
mlp_publication_root = f'{project_data_root}/publication/'
mlp_training_reports_root = f'{project_data_root}/if_training/reports/'
mlp_training_artifacts_root = f'{project_data_root}/if_training/'
mlp_production_model_root = f'{project_data_root}/production_model/'
```

## Cell 11 (markdown)

```markdown
### Code Cell 5 Guide

**Behavior:** Define DynamoDB table contracts for producer and consumer components.

**Purpose:** Make table schema expectations explicit before provisioning and seeding.

**Required input:** Resolved table names and consistent key/attribute definitions.

**Expected output:** Printed JSON contract map for all required tables.

**Expected result:** Operators can audit schema determinism before any write action.

**Usage instructions:** Review printed contract JSON; correct naming mismatches before deploy step.

**Runnable example:** `list(ddb_table_contracts.keys())`
```

## Cell 12 (code)

```python
ddb_table_contracts = {
    'dpp_config': {'table_name': dpp_config_table_name, 'billing_mode': 'PAY_PER_REQUEST', 'attribute_definitions': [{'AttributeName': 'project_name', 'AttributeType': 'S'}, {'AttributeName': 'job_name_version', 'AttributeType': 'S'}], 'key_schema': [{'AttributeName': 'project_name', 'KeyType': 'HASH'}, {'AttributeName': 'job_name_version', 'KeyType': 'RANGE'}]},
    'mlp_config': {'table_name': mlp_config_table_name, 'billing_mode': 'PAY_PER_REQUEST', 'attribute_definitions': [{'AttributeName': 'ml_project_name', 'AttributeType': 'S'}, {'AttributeName': 'job_name_version', 'AttributeType': 'S'}], 'key_schema': [{'AttributeName': 'ml_project_name', 'KeyType': 'HASH'}, {'AttributeName': 'job_name_version', 'KeyType': 'RANGE'}]},
    'batch_index': {'table_name': batch_index_table_name, 'billing_mode': 'PAY_PER_REQUEST', 'attribute_definitions': [{'AttributeName': 'PK', 'AttributeType': 'S'}, {'AttributeName': 'SK', 'AttributeType': 'S'}], 'key_schema': [{'AttributeName': 'PK', 'KeyType': 'HASH'}, {'AttributeName': 'SK', 'KeyType': 'RANGE'}]},
    'routing': {'table_name': routing_table_name, 'billing_mode': 'PAY_PER_REQUEST', 'attribute_definitions': [{'AttributeName': 'org_key', 'AttributeType': 'S'}], 'key_schema': [{'AttributeName': 'org_key', 'KeyType': 'HASH'}]},
    'processing_lock': {'table_name': processing_lock_table_name, 'billing_mode': 'PAY_PER_REQUEST', 'attribute_definitions': [{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'}], 'key_schema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}]},
    'publication_lock': {'table_name': publication_lock_table_name, 'billing_mode': 'PAY_PER_REQUEST', 'attribute_definitions': [{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'}], 'key_schema': [{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}]},
}
print(json.dumps(ddb_table_contracts, indent=2))
```

## Cell 13 (markdown)

```markdown
### Code Cell 6 Guide

**Behavior:** Normalize bootstrap seed templates and promote pipeline specs to READY with artifact metadata.

**Purpose:** Align DDB seed records with artifact lifecycle and deployment checkpoint semantics.

**Required input:** Concrete replacement values including `code_artifact_s3_uri`, `artifact_build_id`, `artifact_sha256`.

**Expected output:** Patched `bootstrap_seed_items` list with READY deployment fields.

**Expected result:** Downstream consumers receive explicit, auditable code metadata and no hidden defaults.

**Usage instructions:** Run after parameter entry; halt if artifact fields are placeholders.

**Runnable example:** `bootstrap_seed_items[0]['job_name_version'] if bootstrap_seed_items else 'no-items'`
```

## Cell 14 (code)

```python
def _replace_placeholders(value, replacements):
    if isinstance(value, dict):
        return {k: _replace_placeholders(v, replacements) for k, v in value.items()}
    if isinstance(value, list):
        return [_replace_placeholders(v, replacements) for v in value]
    if isinstance(value, str):
        out = value
        for needle, repl in replacements.items():
            out = out.replace(needle, repl)
        return out
    return value

def _promote_pipeline_specs_ready(items):
    out = []
    for item in items:
        patched = dict(item)
        spec = patched.get('spec')
        if isinstance(spec, dict):
            scripts = spec.get('scripts') if isinstance(spec.get('scripts'), dict) else {}
            steps = scripts.get('steps') if isinstance(scripts.get('steps'), dict) else {}
            for step_name, step_spec in steps.items():
                if isinstance(step_spec, dict):
                    step_spec['code_artifact_s3_uri'] = code_artifact_s3_uri
                    step_spec['artifact_build_id'] = artifact_build_id
                    step_spec['artifact_sha256'] = artifact_sha256
                    step_spec['artifact_format'] = artifact_format
                    step_spec['code_metadata'] = {'code_artifact_s3_uri': code_artifact_s3_uri, 'artifact_build_id': artifact_build_id, 'artifact_sha256': artifact_sha256, 'artifact_format': artifact_format}
            if steps:
                spec['deployment_status'] = 'READY'
                spec['deployment_checkpoint'] = 'steady_state_ready'
                spec['deployment_last_build_id'] = artifact_build_id
                spec['deployment_last_error'] = ''
                spec['deployment_updated_at'] = UTC_NOW
        out.append(patched)
    return out

bootstrap_seed_items = create_cfg._build_bootstrap_items(project_name=project_name, feature_spec_version=feature_spec_version, owner=owner or 'ndr-team')
replacements = {'<project_name>': project_name, '<feature_spec_version>': feature_spec_version, '<bucket>': default_bucket, 's3://REPLACE_ME': f's3://{default_bucket}' if default_bucket else 's3://'}
bootstrap_seed_items = [_replace_placeholders(item, replacements) for item in bootstrap_seed_items]

def _apply_workload_inputs(items):
    for item in items:
        job_name = str(item.get('job_name_version', '')).split('#', 1)[0]
        spec = item.get('spec') if isinstance(item.get('spec'), dict) else {}
        if job_name == 'machine_inventory_unload':
            redshift = spec['source']['redshift']
            redshift.update({'cluster_identifier': machine_inventory_cluster_identifier, 'database': machine_inventory_database, 'secret_arn': machine_inventory_secret_arn, 'region': aws_region, 'iam_role': machine_inventory_iam_role, 'sql': f'SELECT ip_address, machine_name FROM {machine_inventory_source_table}'})
        elif job_name == 'inference_predictions':
            spec['model']['endpoint_name'] = inference_endpoint_name
    return items

bootstrap_seed_items = _apply_workload_inputs(bootstrap_seed_items)
print('bootstrap items:', len(bootstrap_seed_items))
```

## Cell 15 (markdown)

```markdown
### Code Cell 7 Guide

**Behavior:** Build reciprocal DPP/MLP configuration items and S3 roots records.

**Purpose:** Create deterministic producer/consumer contract items for both config tables.

**Required input:** Project/spec identifiers, owner, ml project list, and root dictionaries.

**Expected output:** Four in-memory item payloads for DDB seed plan assembly.

**Expected result:** Config tables can be seeded idempotently with explicit relationship metadata.

**Usage instructions:** Review generated dictionaries before final seed plan composition.

**Runnable example:** `dpp_reciprocal_item['job_name_version']`
```

## Cell 16 (code)

```python
job_name_version = f'project_parameters#{feature_spec_version}'
dpp_s3_roots = {'delta_root': dpp_delta_root, 'pair_counts_root': dpp_pair_counts_root, 'fg_a_root': dpp_fg_a_root, 'fg_b_root': dpp_fg_b_root, 'fg_c_root': dpp_fg_c_root, 'machine_inventory_root': dpp_machine_inventory_root}
mlp_s3_roots = {'predictions_root': mlp_predictions_root, 'prediction_join_root': mlp_prediction_join_root, 'publication_root': mlp_publication_root, 'training_reports_root': mlp_training_reports_root, 'training_artifacts_root': mlp_training_artifacts_root, 'production_model_root': mlp_production_model_root}

dpp_reciprocal_item = {'project_name': project_name, 'job_name_version': job_name_version, 'ml_project_names': ml_project_names, 'updated_at': UTC_NOW, 'owner': owner, 'spec': {'project_name': project_name, 'ml_project_names': ml_project_names}}
dpp_s3_roots_item = {'project_name': project_name, 'job_name_version': 'S3_ROOTS', 'roots': dpp_s3_roots, 'updated_at': UTC_NOW, 'owner': owner}
mlp_reciprocal_item = {'ml_project_name': ml_project_name, 'job_name_version': job_name_version, 'project_name': project_name, 'updated_at': UTC_NOW, 'owner': owner, 'spec': {'project_name': project_name, 'ml_project_name': ml_project_name}}
mlp_s3_roots_item = {'ml_project_name': ml_project_name, 'job_name_version': 'S3_ROOTS', 'project_name': project_name, 'roots': mlp_s3_roots, 'updated_at': UTC_NOW, 'owner': owner}
```

## Cell 17 (markdown)

```markdown
### Code Cell 8 Guide

**Behavior:** Construct code path/code metadata maps and full DDB seed payload plan.

**Purpose:** Ensure every pipeline step receives the same immutable artifact contract payload.

**Required input:** Non-placeholder artifact metadata and batch/routing context fields.

**Expected output:** `ddb_seed_items_plan` plus printed per-table seed counts.

**Expected result:** Contract payloads remain minimal, deterministic, retry-safe, and rollback-auditable.

**Usage instructions:** Confirm all counts are expected and code metadata values are concrete.

**Runnable example:** `{k: len(v) for k, v in ddb_seed_items_plan.items()}`
```

## Cell 18 (code)

```python
dpp_code_paths = {step_key: code_artifact_s3_uri for step_key in DPP_CODE_STEP_KEYS}
mlp_code_paths = {step_key: code_artifact_s3_uri for step_key in MLP_CODE_STEP_KEYS}
dpp_code_metadata = {step_key: {'code_artifact_s3_uri': code_artifact_s3_uri, 'artifact_build_id': artifact_build_id, 'artifact_sha256': artifact_sha256, 'artifact_format': artifact_format} for step_key in DPP_CODE_STEP_KEYS}
mlp_code_metadata = {step_key: {'code_artifact_s3_uri': code_artifact_s3_uri, 'artifact_build_id': artifact_build_id, 'artifact_sha256': artifact_sha256, 'artifact_format': artifact_format} for step_key in MLP_CODE_STEP_KEYS}

s3_prefixes_dpp = {'delta': '', 'pair_counts': '', 'fg_a': '', 'fg_a_subpaths': {'features': '', 'metadata': ''}, 'fg_c': '', 'machine_inventory': '', 'fg_b': {'machines_manifest': '', 'machines_unload_for_update': '', 'machines_base_stats': '', 'segment_base_stats': ''}, 'code': dpp_code_paths, 'code_metadata': dpp_code_metadata}
s3_prefixes_mlp_branch = {'predictions': '', 'prediction_join': '', 'publication': '', 'training_events': {'training_reports': '', 'training_artifacts': ''}, 'production_artifacts': {'inference_model': ''}, 'code': mlp_code_paths, 'code_metadata': mlp_code_metadata}
s3_prefixes_mlp = {ml_project_name: s3_prefixes_mlp_branch} if ml_project_name else {}

batch_index_direct_item = {'PK': project_name, 'SK': batch_id, 'batch_id': batch_id, 'date_partition': date_partition, 'hour': hour, 'within_hour_run_number': within_hour_run_number, 'etl_ts': etl_ts, 'org1': org1, 'org2': org2, 'raw_parsed_logs_s3_prefix': raw_parsed_logs_s3_prefix, 'ml_project_names': ml_project_names, 's3_prefixes': {'dpp': s3_prefixes_dpp, 'mlp': s3_prefixes_mlp}, 'rt_flow_status': 'planned', 'backfill_status': 'planned', 'source_mode': 'batch_index', 'last_updated_at': UTC_NOW}
reverse_sk = f'{date_partition}#{hour}#{within_hour_run_number}' if date_partition and hour and within_hour_run_number else ''
batch_index_reverse_item = {'PK': project_name, 'SK': reverse_sk, 'batch_id': batch_id, 'batch_lookup_sk': batch_id, 'date_partition': date_partition, 'hour': hour, 'within_hour_run_number': within_hour_run_number, 'etl_ts': etl_ts, 'org1': org1, 'org2': org2}
routing_item = {'org_key': f'{org1}#{org2}' if org1 and org2 else '', 'project_name': project_name, 'ml_project_names': ml_project_names, 'ingestion_prefix': ingestion_prefix, 'feature_spec_version': feature_spec_version, 'updated_at': UTC_NOW}

ddb_seed_items_plan = {'dpp_config': [*bootstrap_seed_items, dpp_reciprocal_item, dpp_s3_roots_item], 'mlp_config': [mlp_reciprocal_item, mlp_s3_roots_item], 'batch_index': [batch_index_direct_item, batch_index_reverse_item], 'routing': [routing_item], 'processing_lock': [], 'publication_lock': []}
print(json.dumps({'seed_counts': {k: len(v) for k, v in ddb_seed_items_plan.items()}}, indent=2))
```

## Cell 19 (markdown)

```markdown
### Code Cell 9 Guide

**Behavior:** Define required S3 bucket prefix schema for ingestion, outputs, artifacts, and observability.

**Purpose:** Provide explicit storage lifecycle contract before provisioning actions.

**Required input:** Target bucket name and agreed prefix list.

**Expected output:** Printed S3 schema plan JSON.

**Expected result:** Operators can validate bucket/prefix coverage against deployment expectations.

**Usage instructions:** Adjust prefixes only if contract changes are approved across producers/consumers.

**Runnable example:** `s3_schema_plan['prefixes'][:3]`
```

## Cell 20 (code)

```python
s3_schema_plan = {'bucket_name': default_bucket, 'prefixes': ['input/raw', 'input/reference', 'output/processed/delta_15m', 'output/processed/fg_a', 'output/processed/pair_counts', 'output/processed/fg_b', 'output/processed/fg_c', 'output/predictions', 'output/prediction_join', 'output/publication', 'artifacts/code', 'artifacts/pipeline', 'feature-store/offline', 'monitoring', 'logs', 'orchestration/readiness/rt_artifact_readiness/v3', 'orchestration/readiness/monthly_fg_b_readiness/v3']}
print(json.dumps(s3_schema_plan, indent=2))
```

## Cell 21 (markdown)

```markdown
### Code Cell 10 Guide

**Behavior:** Declare idempotent deployment helpers for DDB, S3, pipeline upsert, and materialization startup.

**Purpose:** Centralize operational actions with dry-run support and deterministic behavior.

**Required input:** Valid AWS credentials/permissions when `dry_run=False`.

**Expected output:** Reusable helper functions for deployment execution.

**Expected result:** Every run reconciles missing infrastructure and seed records, refreshes deployment-owned pipeline JobSpecs, preserves existing environment-owned records, and fails fast on incompatible table schemas.

**Usage instructions:** Keep `dry_run=True` during review; switch only after readiness checks pass. Run the same reconciliation flow for both new and existing environments.

**Runnable example:** `deploy_ddb(ddb_table_contracts, ddb_seed_items_plan, aws_region, dry_run=True)`
```

## Cell 22 (code)

```python
def _find_unresolved_placeholder(value, path='$'):
    if isinstance(value, dict):
        for key, item in value.items():
            found = _find_unresolved_placeholder(item, f'{path}.{key}')
            if found: return found
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found = _find_unresolved_placeholder(item, f'{path}[{index}]')
            if found: return found
    elif isinstance(value, str) and any(marker in value for marker in ('REPLACE_ME', '<REPLACE_', '${')):
        return f'{path}={value!r}'
    return ''

def validate_deployment_preflight(required_inputs, seed_plan):
    missing = [name for name, value in required_inputs.items() if value is None or str(value).strip() == '']
    if missing: raise ValueError(f'Missing required deployment inputs: {missing}')
    unresolved = _find_unresolved_placeholder(seed_plan)
    if unresolved: raise ValueError(f'Unresolved deployment placeholder: {unresolved}')

def _normalized_schema(items):
    return sorted((item['AttributeName'], item.get('AttributeType') or item.get('KeyType')) for item in items)

def _validate_existing_ddb_table(table, spec):
    if _normalized_schema(table['AttributeDefinitions']) != _normalized_schema(spec['attribute_definitions']):
        raise ValueError(f"Existing table {spec['table_name']} has incompatible attribute definitions")
    if _normalized_schema(table['KeySchema']) != _normalized_schema(spec['key_schema']):
        raise ValueError(f"Existing table {spec['table_name']} has incompatible key schema")

def ensure_ddb_table(ddb_client, spec, dry_run=True):
    if dry_run:
        print('[DRY RUN] reconcile table', spec['table_name']); return
    try:
        existing = ddb_client.describe_table(TableName=spec['table_name'])['Table']
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') != 'ResourceNotFoundException': raise
        ddb_client.create_table(TableName=spec['table_name'], BillingMode=spec['billing_mode'], KeySchema=spec['key_schema'], AttributeDefinitions=spec['attribute_definitions'])
        ddb_client.get_waiter('table_exists').wait(TableName=spec['table_name'])
        return
    _validate_existing_ddb_table(existing, spec)

def _is_deployment_owned_seed(logical_name, item):
    base_name = str(item.get('job_name_version', '')).split('#', 1)[0]
    return logical_name == 'dpp_config' and base_name != 'project_parameters' and ('spec' in item or base_name.startswith('pipeline_'))

def seed_table_items(ddb_resource, logical_name, contract, items, dry_run=True):
    if dry_run:
        print(f"[DRY RUN] reconcile {len(items)} items -> {contract['table_name']}"); return
    table = ddb_resource.Table(contract['table_name'])
    key_names = [item['AttributeName'] for item in contract['key_schema']]
    expression_names = {f'#key{i}': key for i, key in enumerate(key_names)}
    insert_only_condition = ' AND '.join(f'attribute_not_exists(#key{i})' for i in range(len(key_names)))
    for item in items:
        if _is_deployment_owned_seed(logical_name, item):
            table.put_item(Item=item)
            continue
        try:
            table.put_item(Item=item, ConditionExpression=insert_only_condition, ExpressionAttributeNames=expression_names)
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') != 'ConditionalCheckFailedException': raise
            print('PRESERVED existing seed', contract['table_name'], {key: item[key] for key in key_names})

def deploy_ddb(table_contracts, seed_plan, region_name, dry_run=True):
    ddb_client = boto3.client('dynamodb', region_name=region_name)
    ddb_resource = boto3.resource('dynamodb', region_name=region_name)
    for logical_name, contract in table_contracts.items():
        ensure_ddb_table(ddb_client, contract, dry_run=dry_run)
        seed_table_items(ddb_resource, logical_name, contract, seed_plan.get(logical_name, []), dry_run=dry_run)

def _stable_marker_value(value):
    volatile_keys = {'applied_at', 'deployment_updated_at', 'updated_at'}
    if isinstance(value, dict):
        return {key: _stable_marker_value(item) for key, item in sorted(value.items()) if key not in volatile_keys}
    if isinstance(value, list):
        return [_stable_marker_value(item) for item in value]
    return value

def build_deployment_marker(project_name, environment_name, feature_spec_version, deployment_revision, artifact_build_id, artifact_sha256, code_artifact_s3_uri, table_contracts, s3_schema, pipeline_names, pipeline_definitions, pipeline_job_specs, rendered_step_functions, eventbridge_config, applied_at):
    manifest = _stable_marker_value({
        'deployment_revision': deployment_revision,
        'feature_spec_version': feature_spec_version,
        'artifact_build_id': artifact_build_id,
        'artifact_sha256': artifact_sha256,
        'code_artifact_s3_uri': code_artifact_s3_uri,
        'ddb_table_contracts': table_contracts,
        's3_schema': s3_schema,
        'pipeline_names': sorted(pipeline_names),
        'pipeline_definitions': pipeline_definitions,
        'pipeline_job_specs': pipeline_job_specs,
        'rendered_step_functions': rendered_step_functions,
        'eventbridge_config': eventbridge_config,
    })
    fingerprint = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()
    return {
        'project_name': project_name,
        'job_name_version': f'DEPLOYMENT_MARKER#{environment_name}#{feature_spec_version}',
        'marker_schema_version': 'NdrDeploymentMarker.v1',
        'deployment_revision': deployment_revision,
        'deployment_fingerprint': fingerprint,
        'feature_spec_version': feature_spec_version,
        'artifact_build_id': artifact_build_id,
        'artifact_sha256': artifact_sha256,
        'code_artifact_s3_uri': code_artifact_s3_uri,
        'applied_at': applied_at,
    }

def persist_deployment_marker(ddb_resource, table_name, marker, dry_run=True):
    key = {'project_name': marker['project_name'], 'job_name_version': marker['job_name_version']}
    if dry_run:
        print('[DRY RUN] persist deployment marker', key, marker['deployment_fingerprint'])
        return {'action': 'dry-run', 'previous_fingerprint': '', 'deployment_fingerprint': marker['deployment_fingerprint']}
    table = ddb_resource.Table(table_name)
    existing = table.get_item(Key=key).get('Item')
    previous_fingerprint = (existing or {}).get('deployment_fingerprint', '')
    action = 'created' if existing is None else ('unchanged' if previous_fingerprint == marker['deployment_fingerprint'] else 'updated')
    table.put_item(Item=marker)
    print(action.upper(), 'deployment marker', key, marker['deployment_fingerprint'])
    return {'action': action, 'previous_fingerprint': previous_fingerprint, 'deployment_fingerprint': marker['deployment_fingerprint']}

def ensure_s3_schema(schema, region_name, dry_run=True):
    bucket = schema['bucket_name']
    s3 = boto3.client('s3', region_name=region_name)
    if dry_run:
        print('[DRY RUN] ensure bucket/prefixes for', bucket)
        for p in schema['prefixes']: print(f'[DRY RUN] s3://{bucket}/{p}/')
        return
    existing = {b['Name'] for b in s3.list_buckets().get('Buckets', [])}
    if bucket and bucket not in existing:
        params = {'Bucket': bucket}
        if region_name != 'us-east-1': params['CreateBucketConfiguration'] = {'LocationConstraint': region_name}
        s3.create_bucket(**params)
    for p in schema['prefixes']: s3.put_object(Bucket=bucket, Key=f"{p.rstrip('/')}/")

def reconcile_event_bus(events_client, event_bus_name, dry_run=True):
    if dry_run:
        print('[DRY RUN] reconcile event bus', event_bus_name); return
    try:
        events_client.describe_event_bus(Name=event_bus_name)
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') != 'ResourceNotFoundException': raise
        events_client.create_event_bus(Name=event_bus_name)

def upsert_pipelines(project_name_for_contracts, feature_spec_version_for_contracts, role_arn, bucket, region_name, dry_run=True):
    definitions = {}
    for pipeline_job_name, builder in PIPELINE_BUILDERS.items():
        pipeline_name = f"{project_name_for_contracts}-{feature_spec_version_for_contracts}-{pipeline_job_name}"
        if dry_run:
            print('[DRY RUN] upsert pipeline', pipeline_name); continue
        pipeline = builder(pipeline_name=pipeline_name, role_arn=role_arn, default_bucket=bucket, region_name=region_name, project_name_for_contracts=project_name_for_contracts, feature_spec_version_for_contracts=feature_spec_version_for_contracts)
        definitions[pipeline_name] = pipeline.definition()
        pipeline.upsert(role_arn=role_arn)
    return definitions

def run_artifact_lifecycle(project_name, feature_spec_version, artifact_build_id, region_name, dpp_config_table_name, dry_run=True):
    paths = {'build': '/tmp/code_bundle_build_output.json', 'validate': '/tmp/code_artifact_validate_report.json', 'smoke': '/tmp/code_smoke_validate_report.json'}
    commands = [
        [sys.executable, '-m', 'ndr.scripts.run_code_bundle_build', '--project-name', project_name, '--feature-spec-version', feature_spec_version, '--artifact-build-id', artifact_build_id, '--region-name', region_name, '--dpp-config-table-name', dpp_config_table_name, '--manifest-out', paths['build']],
        [sys.executable, '-m', 'ndr.scripts.run_code_artifact_validate', '--project-name', project_name, '--feature-spec-version', feature_spec_version, '--artifact-build-id', artifact_build_id, '--region-name', region_name, '--build-manifest-in', paths['build'], '--validation-report-out', paths['validate']],
        [sys.executable, '-m', 'ndr.scripts.run_code_smoke_validate', '--project-name', project_name, '--feature-spec-version', feature_spec_version, '--artifact-build-id', artifact_build_id, '--region-name', region_name, '--build-manifest-in', paths['build'], '--validation-report-in', paths['validate'], '--smoke-report-out', paths['smoke']],
    ]
    if dry_run:
        for command in commands: print('[DRY RUN]', ' '.join(command))
        return {}
    for command in commands: subprocess.run(command, check=True)
    reports = {name: json.loads(Path(path).read_text(encoding='utf-8')) for name, path in paths.items()}
    if reports['validate'].get('status') != 'PASS' or reports['smoke'].get('status') != 'PASS': raise ValueError('Artifact lifecycle validation did not pass')
    return reports

def apply_artifact_manifest(items, build_manifest):
    artifacts = {(item['pipeline_job_name'], item['step_name']): item for item in build_manifest.get('step_artifacts', [])}
    for item in items:
        job_name = str(item.get('job_name_version', '')).split('#', 1)[0]
        spec = item.get('spec') if isinstance(item.get('spec'), dict) else {}
        steps = ((spec.get('scripts') or {}).get('steps') or {})
        for step_name, step_spec in steps.items():
            artifact = artifacts.get((job_name, step_name))
            if artifact:
                step_spec.update({key: artifact[key] for key in ('code_artifact_s3_uri', 'artifact_build_id', 'artifact_sha256', 'artifact_format')})
                step_spec['code_metadata'] = {key: artifact[key] for key in ('code_artifact_s3_uri', 'artifact_build_id', 'artifact_sha256', 'artifact_format')}
        if steps and all((job_name, step_name) in artifacts for step_name in steps):
            spec.update({'deployment_status': 'READY', 'deployment_checkpoint': 'steady_state_ready', 'deployment_last_build_id': build_manifest['artifact_build_id'], 'deployment_last_error': '', 'deployment_updated_at': UTC_NOW})
    return items
```

## Cell 23 (markdown)

```markdown
### Code Cell 11 Guide

**Behavior:** Validate all operator inputs and seeded workload configuration before the first AWS write.

**Purpose:** Prevent partial deployments caused by empty inputs or unresolved workload placeholders.

**Required input:** Concrete environment, IAM-role, workload, batch, and artifact-build inputs from prior cells.

**Expected output:** A deployment-preflight pass message or an explicit blocking validation error.

**Expected result:** Reconciliation cannot begin while required inputs or unresolved `REPLACE_ME` placeholders remain.

**Usage instructions:** Populate every required input and rerun this cell until preflight passes.

**Runnable example:** `validate_deployment_preflight(required_deployment_inputs, ddb_seed_items_plan)`
```

## Cell 24 (code)

```python
required_deployment_inputs = {
    'aws_account_id': aws_account_id, 'aws_region': aws_region, 'project_name': project_name, 'feature_spec_version': feature_spec_version, 'environment_name': environment_name,
    'sagemaker_role_arn': sagemaker_role_arn, 'step_functions_role_arn': step_functions_role_arn, 'eventbridge_invoke_role_arn': eventbridge_invoke_role_arn, 'default_bucket': default_bucket,
    'ml_project_name': ml_project_name, 'org1': org1, 'org2': org2, 'ingestion_prefix': ingestion_prefix, 'batch_id': batch_id, 'date_partition': date_partition, 'hour': hour,
    'within_hour_run_number': within_hour_run_number, 'etl_ts': etl_ts, 'raw_parsed_logs_s3_prefix': raw_parsed_logs_s3_prefix, 'reference_month': reference_month,
    'inference_endpoint_name': inference_endpoint_name, 'machine_inventory_cluster_identifier': machine_inventory_cluster_identifier, 'machine_inventory_database': machine_inventory_database,
    'machine_inventory_secret_arn': machine_inventory_secret_arn, 'machine_inventory_iam_role': machine_inventory_iam_role, 'machine_inventory_source_table': machine_inventory_source_table,
    'artifact_build_id': artifact_build_id,
}
validate_deployment_preflight(required_deployment_inputs, ddb_seed_items_plan)
print('Deployment preflight passed.')
```

## Cell 25 (markdown)

```markdown
### Code Cell 12 Guide

**Behavior:** Execute full deployment orchestration in dry-run mode by default.

**Purpose:** Provide a safe end-to-end reconciliation rehearsal for table/schema/pipeline/materialization steps.

**Required input:** All prior configuration cells executed with reviewed values.

**Expected output:** Dry-run logs for each reconciliation phase and completion message.

**Expected result:** The same run creates missing resources, updates deployment-owned resources, and preserves correct existing environment-owned records.

**Usage instructions:** Run unchanged for both new and previously deployed environments. Set `DRY_RUN=False` only after validations and approvals are complete.

**Runnable example:** `DRY_RUN = True`
```

## Cell 26 (code)

```python
DRY_RUN = True
event_bus_name = f'{project_name}-{environment_name}-events'
deploy_ddb(ddb_table_contracts, ddb_seed_items_plan, region_name=aws_region, dry_run=DRY_RUN)
ensure_s3_schema(s3_schema_plan, region_name=aws_region, dry_run=DRY_RUN)
reconcile_event_bus(boto3.client('events', region_name=aws_region), event_bus_name, dry_run=DRY_RUN)
artifact_lifecycle_reports = run_artifact_lifecycle(project_name, feature_spec_version, artifact_build_id, aws_region, dpp_config_table_name, dry_run=DRY_RUN)
if artifact_lifecycle_reports:
    artifact_sha256 = artifact_lifecycle_reports['build']['artifact_sha256']
    code_artifact_s3_uri = artifact_lifecycle_reports['build']['step_artifacts'][0]['code_artifact_s3_uri']
    ddb_seed_items_plan['dpp_config'] = apply_artifact_manifest(ddb_seed_items_plan['dpp_config'], artifact_lifecycle_reports['build'])
    deploy_ddb(ddb_table_contracts, ddb_seed_items_plan, region_name=aws_region, dry_run=False)
pipeline_definitions = upsert_pipelines(project_name_for_contracts=project_name, feature_spec_version_for_contracts=feature_spec_version, role_arn=sagemaker_role_arn, bucket=default_bucket, region_name=aws_region, dry_run=DRY_RUN)
print('Core reconciliation deployment flow complete.')
```

## Cell 27 (markdown)

```markdown
### Code Cell 12 Guide

**Behavior:** Render and upsert Step Functions, reconcile EventBridge schedules, and run the initial bootstrap materialization to completion.

**Purpose:** Replace manual GUI paste steps with idempotent create/update automation.

**Required input:** Valid AWS region/account, externally provisioned IAM role ARNs, deterministic substitutions, and bootstrap runtime inputs.

**Expected output:** Per-state-machine CREATED/UPDATED log lines and an execution summary list.

**Expected result:** Orchestrator state machines and schedules are reconciled consistently, and bootstrap materialization succeeds before marker persistence.

**Usage instructions:** Run with `dry_run=True` first, then set `DRY_RUN=False`; IAM roles remain externally managed prerequisites validated by preflight.

**Runnable example:** `results = upsert_step_functions(dry_run=True); len(results)`
```

## Cell 28 (code)

```python
from pathlib import Path

from ndr.orchestration.step_functions_validation import (
    ensure_required_substitutions,
    render_and_validate_template,
    validate_state_machine_definition,
)

SFN_TEMPLATE_DIR = Path("docs/step_functions_jsonata")

def _state_machine_name(suffix):
    return f'{project_name}-{environment_name}-{feature_spec_version}-{suffix}'

def _state_machine_arn(suffix):
    return f'arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:{_state_machine_name(suffix)}'

STATE_MACHINE_DEPLOYMENT = {
    "sfn_ndr_15m_features_inference.json": {"name": _state_machine_name("15m-features-inference"), "role_arn": step_functions_role_arn},
    "sfn_ndr_training_orchestrator.json": {"name": _state_machine_name("training-orchestrator"), "role_arn": step_functions_role_arn},
    "sfn_ndr_prediction_publication.json": {"name": _state_machine_name("prediction-publication"), "role_arn": step_functions_role_arn},
    "sfn_ndr_backfill_reprocessing.json": {"name": _state_machine_name("backfill-reprocessing"), "role_arn": step_functions_role_arn},
    "sfn_ndr_monthly_fg_b_baselines.json": {"name": _state_machine_name("monthly-fg-b-baselines"), "role_arn": step_functions_role_arn},
    "sfn_ndr_initial_deployment_bootstrap.json": {"name": _state_machine_name("initial-deployment-bootstrap"), "role_arn": step_functions_role_arn},
    "sfn_ndr_code_deployment_orchestrator.json": {"name": _state_machine_name("code-deployment-orchestrator"), "role_arn": step_functions_role_arn},
}

def _pipeline_name(job_name: str) -> str:
    return f"{project_name}-{feature_spec_version}-{job_name}"

SFN_SUBSTITUTIONS = {
    "ProjectRoutingTableName": routing_table_name,
    "DppConfigTableName": dpp_config_table_name,
    "MlpConfigTableName": mlp_config_table_name,
    "BatchIndexTableName": batch_index_table_name,
    "LockTableName": processing_lock_table_name,
    "PublicationLockTableName": publication_lock_table_name,
    "DefaultFeatureSpecVersion": feature_spec_version,
    "ArtifactsBucketName": default_bucket,
    "EventBusName": event_bus_name,
    "PipelineName15m": _pipeline_name("pipeline_15m_streaming"),
    "PipelineName15mDependent": _pipeline_name("pipeline_15m_dependent"),
    "PipelineNameInference": _pipeline_name("pipeline_inference_predictions"),
    "PipelineNameRtReadiness": _pipeline_name("pipeline_rt_readiness"),
    "PipelineNameBackfillHistoricalExtractor": _pipeline_name("pipeline_backfill_historical_extractor"),
    "PipelineNameBackfill15m": _pipeline_name("pipeline_backfill_15m_reprocessing"),
    "PipelineNameFGB": _pipeline_name("pipeline_fg_b_baseline"),
    "PipelineNameMonthlyReadiness": _pipeline_name("pipeline_monthly_readiness"),
    "PipelineNamePredictionJoin": _pipeline_name("pipeline_prediction_feature_join"),
    "PipelineNameIFTraining": _pipeline_name("pipeline_if_training"),
    "PipelineNameMachineInventory": _pipeline_name("pipeline_machine_inventory_unload"),
    "PipelineNameCodeBundleBuild": _pipeline_name("pipeline_code_bundle_build"),
    "PipelineNameCodeArtifactValidate": _pipeline_name("pipeline_code_artifact_validate"),
    "PipelineNameCodeSmokeValidate": _pipeline_name("pipeline_code_smoke_validate"),
    "BootstrapStateMachineArn": _state_machine_arn("initial-deployment-bootstrap"),
    "BackfillStateMachineArn": _state_machine_arn("backfill-reprocessing"),
    "MonthlyStateMachineArn": _state_machine_arn("monthly-fg-b-baselines"),
    "PredictionPublicationStateMachineArn": _state_machine_arn("prediction-publication"),
}

REQUIRED_SFN_SUBSTITUTIONS = [
    "ProjectRoutingTableName",
    "DppConfigTableName",
    "MlpConfigTableName",
    "BatchIndexTableName",
    "LockTableName",
    "PublicationLockTableName",
    "DefaultFeatureSpecVersion",
    "ArtifactsBucketName",
    "EventBusName",
    "PipelineName15m",
    "PipelineName15mDependent",
    "PipelineNameInference",
    "PipelineNameRtReadiness",
    "PipelineNameBackfillHistoricalExtractor",
    "PipelineNameBackfill15m",
    "PipelineNameFGB",
    "PipelineNameMonthlyReadiness",
    "PipelineNamePredictionJoin",
    "PipelineNameIFTraining",
    "PipelineNameMachineInventory",
    "PipelineNameCodeBundleBuild",
    "PipelineNameCodeArtifactValidate",
    "PipelineNameCodeSmokeValidate",
    "BootstrapStateMachineArn",
    "BackfillStateMachineArn",
    "MonthlyStateMachineArn",
    "PredictionPublicationStateMachineArn",
]

def _find_state_machine_arn(sfn_client, name):
    paginator = sfn_client.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for item in page.get("stateMachines", []):
            if item["name"] == name:
                return item["stateMachineArn"]
    return None

def validate_step_function_templates(substitutions):
    ensure_required_substitutions(substitutions, REQUIRED_SFN_SUBSTITUTIONS)
    rendered = {}
    for template_file in STATE_MACHINE_DEPLOYMENT:
        template_path = SFN_TEMPLATE_DIR / template_file
        if not template_path.exists():
            raise FileNotFoundError(f"Missing Step Functions template: {template_path}")
        rendered_text = render_and_validate_template(template_path.read_text(encoding="utf-8"), substitutions)
        validate_state_machine_definition(rendered_text)
        rendered[template_file] = rendered_text
    return rendered

def upsert_step_functions(dry_run=True, deployment_targets=None):
    sfn_client = boto3.client("stepfunctions", region_name=aws_region)
    ordered_targets = deployment_targets or list(STATE_MACHINE_DEPLOYMENT.keys())
    unknown = [name for name in ordered_targets if name not in STATE_MACHINE_DEPLOYMENT]
    if unknown:
        raise ValueError(f"Unknown Step Functions templates requested: {unknown}")

    rendered_templates = validate_step_function_templates(SFN_SUBSTITUTIONS)

    results = []
    for template_file in ordered_targets:
        deployment = STATE_MACHINE_DEPLOYMENT[template_file]
        name = deployment["name"]
        role_arn = deployment["role_arn"]
        rendered = rendered_templates[template_file]
        existing_arn = _find_state_machine_arn(sfn_client, name)
        if dry_run:
            action = "UPDATE" if existing_arn else "CREATE"
            print(f"[DRY RUN] {action} {name} <- {template_file}")
            results.append({"name": name, "template": template_file, "action": action.lower(), "state_machine_arn": existing_arn or ""})
            continue
        if existing_arn:
            sfn_client.update_state_machine(
                stateMachineArn=existing_arn,
                definition=rendered,
                roleArn=role_arn,
            )
            print(f"UPDATED {name}")
            results.append({"name": name, "template": template_file, "action": "updated", "state_machine_arn": existing_arn})
        else:
            created = sfn_client.create_state_machine(
                name=name,
                definition=rendered,
                roleArn=role_arn,
                type="STANDARD",
            )
            print(f"CREATED {name}")
            results.append({"name": name, "template": template_file, "action": "created", "state_machine_arn": created["stateMachineArn"]})
    return results

step_function_results = upsert_step_functions(dry_run=DRY_RUN)
print(json.dumps(step_function_results, indent=2))

def _event_bus_kwargs(rule):
    return {'EventBusName': rule['event_bus_name']} if rule.get('event_bus_name') else {}

def _eventbridge_target(rule):
    target = {'Id': rule['name'], 'Arn': rule['target_arn'], 'RoleArn': rule['role_arn']}
    if rule.get('input') is not None:
        target['Input'] = json.dumps(rule['input'], sort_keys=True)
    if rule.get('input_path') is not None:
        target['InputPath'] = rule['input_path']
    if 'Input' in target and 'InputPath' in target:
        raise ValueError(f"EventBridge rule {rule['name']} cannot define both input and input_path")
    return target

def reconcile_eventbridge_rules(events_client, rules, dry_run=True):
    for rule in rules:
        has_schedule = bool(rule.get('schedule_expression'))
        has_pattern = bool(rule.get('event_pattern'))
        if has_schedule == has_pattern:
            raise ValueError(f"EventBridge rule {rule['name']} must define exactly one schedule_expression or event_pattern")
        if dry_run:
            print('[DRY RUN] reconcile EventBridge rule', rule['name']); continue
        rule_args = {'Name': rule['name'], 'State': 'ENABLED', **_event_bus_kwargs(rule)}
        if has_schedule:
            rule_args['ScheduleExpression'] = rule['schedule_expression']
        else:
            rule_args['EventPattern'] = json.dumps(rule['event_pattern'], sort_keys=True)
        events_client.put_rule(**rule_args)
        events_client.put_targets(Rule=rule['name'], Targets=[_eventbridge_target(rule)], **_event_bus_kwargs(rule))

def remove_eventbridge_rules(events_client, rules, dry_run=True):
    for rule in rules:
        if dry_run:
            print('[DRY RUN] remove legacy EventBridge rule', rule['name']); continue
        event_bus_kwargs = _event_bus_kwargs(rule)
        try:
            target_ids = [item['Id'] for item in events_client.list_targets_by_rule(Rule=rule['name'], **event_bus_kwargs).get('Targets', [])]
            if target_ids:
                events_client.remove_targets(Rule=rule['name'], Ids=target_ids, **event_bus_kwargs)
            events_client.delete_rule(Name=rule['name'], **event_bus_kwargs)
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') != 'ResourceNotFoundException':
                raise

def _wait_for_state_machine_execution(sfn_client, execution_arn, timeout_seconds=21600, poll_seconds=15):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = sfn_client.describe_execution(executionArn=execution_arn)['status']
        if status == 'SUCCEEDED': return status
        if status in {'FAILED', 'TIMED_OUT', 'ABORTED'}: raise RuntimeError(f'Bootstrap execution {execution_arn} ended with {status}')
        time.sleep(poll_seconds)
    raise TimeoutError(f'Bootstrap execution {execution_arn} did not finish within {timeout_seconds} seconds')

def _find_execution_arn(sfn_client, state_machine_arn, execution_name):
    for status_filter in ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'):
        for item in sfn_client.list_executions(stateMachineArn=state_machine_arn, statusFilter=status_filter, maxResults=100).get('executions', []):
            if item['name'] == execution_name: return item['executionArn']
    raise RuntimeError(f'Existing bootstrap execution not found: {execution_name}')

def start_initial_materialization_runs(sfn_client, bootstrap_state_machine_arn, execution_name, execution_input, dry_run=True):
    if dry_run:
        print('[DRY RUN] start bootstrap materialization', execution_name); return {'action': 'dry-run'}
    try:
        response = sfn_client.start_execution(stateMachineArn=bootstrap_state_machine_arn, name=execution_name, input=json.dumps(execution_input, sort_keys=True))
        execution_arn, action = response['executionArn'], 'started'
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') != 'ExecutionAlreadyExists': raise
        execution_arn, action = _find_execution_arn(sfn_client, bootstrap_state_machine_arn, execution_name), 'preserved'
        previous_status = sfn_client.describe_execution(executionArn=execution_arn)['status']
        if previous_status in {'FAILED', 'TIMED_OUT', 'ABORTED'}:
            retry_name = f'{execution_name[:60]}-retry-{int(time.time())}'
            response = sfn_client.start_execution(stateMachineArn=bootstrap_state_machine_arn, name=retry_name, input=json.dumps(execution_input, sort_keys=True))
            execution_arn, action = response['executionArn'], 'retried'
    _wait_for_state_machine_execution(sfn_client, execution_arn)
    return {'action': action, 'execution_arn': execution_arn, 'status': 'SUCCEEDED'}

eventbridge_rules = [
    {'name': _state_machine_name('trigger-15m-ingestion'), 'event_bus_name': event_bus_name, 'event_pattern': {'source': ['ndr.ingestion'], 'detail-type': ['NdrRawParsedLogsBatchCompleted'], 'detail': {'project_name': [{'exists': True}], 'data_source_name': [{'exists': True}], 'batch_id': [{'exists': True}], 'raw_parsed_logs_s3_prefix': [{'exists': True}], 'timestamp': [{'exists': True}], 'feature_spec_version': [{'exists': True}]}}, 'target_arn': _state_machine_arn('15m-features-inference'), 'role_arn': eventbridge_invoke_role_arn, 'input_path': '$.detail'},
    {'name': _state_machine_name('schedule-monthly'), 'schedule_expression': 'cron(0 0 1 * ? *)', 'target_arn': _state_machine_arn('monthly-fg-b-baselines'), 'role_arn': eventbridge_invoke_role_arn, 'input': {'project_name': project_name, 'feature_spec_version': feature_spec_version}},
]
legacy_eventbridge_rules = [
    {'name': _state_machine_name('schedule-15m'), 'event_bus_name': event_bus_name},
    {'name': _state_machine_name('schedule-monthly'), 'event_bus_name': event_bus_name},
]
events_client = boto3.client('events', region_name=aws_region)
reconcile_eventbridge_rules(events_client, eventbridge_rules, dry_run=DRY_RUN)
remove_eventbridge_rules(events_client, legacy_eventbridge_rules, dry_run=DRY_RUN)
bootstrap_execution_result = start_initial_materialization_runs(boto3.client('stepfunctions', region_name=aws_region), _state_machine_arn('initial-deployment-bootstrap'), f'bootstrap-{environment_name}-{feature_spec_version}-r{deployment_revision}', {'project_name': project_name, 'feature_spec_version': feature_spec_version, 'ml_project_name': ml_project_name, 'raw_parsed_logs_s3_prefix': raw_parsed_logs_s3_prefix, 'reference_month': reference_month}, dry_run=DRY_RUN)
print(json.dumps(bootstrap_execution_result, indent=2))

# Optional targeted rollout pattern (recommended):
# 1) upsert_step_functions(dry_run=True)
# 2) upsert_step_functions(dry_run=False, deployment_targets=["sfn_ndr_training_orchestrator.json"])
# 3) upsert_step_functions(dry_run=False)
```

## Cell 29 (markdown)

```markdown
### Code Cell 13 Guide

**Behavior:** Run final structural readiness assertions, compute the deterministic deployment fingerprint, and persist the post-success deployment marker.

**Purpose:** Fail fast on contract violations before any production rollout.

**Required input:** Populated `ddb_table_contracts`, `ddb_seed_items_plan`, and `PIPELINE_BUILDERS`.

**Expected output:** Assertion pass message or explicit failure with violating key, followed by the deployment-marker reconciliation result.

**Expected result:** Readiness decisions remain deterministic and the DPP config table records the latest successfully reconciled deployment fingerprint.

**Usage instructions:** Treat any assertion failure as blocking; remediate configuration then rerun. Increment `deployment_revision` only when forcing a new deployment boundary not otherwise represented by the manifest inputs.

**Runnable example:** `print('ready' if not missing_pipeline_jobs else missing_pipeline_jobs)`
```

## Cell 30 (code)

```python
assert all(name in ddb_table_contracts for name in ['dpp_config','mlp_config','batch_index','routing','processing_lock','publication_lock'])
assert all(name in ddb_seed_items_plan for name in ['dpp_config','mlp_config','batch_index','routing'])
seeded_job_names = {item['job_name_version'].split('#',1)[0] for item in ddb_seed_items_plan['dpp_config'] if 'job_name_version' in item}
missing_pipeline_jobs = [name for name in PIPELINE_BUILDERS if name not in seeded_job_names]
assert not missing_pipeline_jobs, missing_pipeline_jobs
for item in ddb_seed_items_plan['dpp_config']:
    spec = item.get('spec')
    if isinstance(spec, dict) and isinstance((spec.get('scripts') or {}).get('steps'), dict):
        assert spec.get('deployment_status') == 'READY', item.get('job_name_version')
assert 's3_prefixes' in batch_index_direct_item and 'dpp' in batch_index_direct_item['s3_prefixes'] and 'mlp' in batch_index_direct_item['s3_prefixes']
print('Structural readiness checks passed.')

rendered_step_functions_for_marker = validate_step_function_templates(SFN_SUBSTITUTIONS)
deployment_marker = build_deployment_marker(
    project_name=project_name,
    environment_name=environment_name,
    feature_spec_version=feature_spec_version,
    deployment_revision=deployment_revision,
    artifact_build_id=artifact_build_id,
    artifact_sha256=artifact_sha256,
    code_artifact_s3_uri=code_artifact_s3_uri,
    table_contracts=ddb_table_contracts,
    s3_schema=s3_schema_plan,
    pipeline_names=PIPELINE_BUILDERS,
    pipeline_definitions=pipeline_definitions,
    pipeline_job_specs=[item for item in ddb_seed_items_plan['dpp_config'] if _is_deployment_owned_seed('dpp_config', item)],
    rendered_step_functions=rendered_step_functions_for_marker,
    eventbridge_config={'event_bus_name': event_bus_name, 'rules': eventbridge_rules},
    applied_at=UTC_NOW,
)
deployment_marker_result = persist_deployment_marker(boto3.resource('dynamodb', region_name=aws_region), dpp_config_table_name, deployment_marker, dry_run=DRY_RUN)
print(json.dumps(deployment_marker_result, indent=2))
```

## Cell 31 (markdown)

```markdown
## Operator completion checklist

1. DDB tables created and seeded.
2. S3 schema created.
3. Artifact values set and promoted.
4. Pipelines upserted.
5. Initial feature/stats materialization runs executed.
6. Step Functions reconciled.
7. Deployment marker persisted after successful structural checks.
```
