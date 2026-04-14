# Canonical End-to-End Deployment Notebook (Markdown Mirror)

This file mirrors `canonical_end_to_end_deployment_plan.ipynb` exactly, with explicit cell boundaries for copy/paste.

## Cell 1 (markdown)

```markdown
# Canonical End-to-End Deployment Notebook (Repaired - conflicts free)

This notebook is rebuilt from code/config sources in `src/ndr` and `tests` only, and provides an executable deployment workflow.
```

## Cell 2 (markdown)

```markdown
## 0) Deployment order and forms

1. Code-only dependency analysis (not docs).
2. Variables/placeholders.
3. DDB contracts + seed dictionaries.
4. S3 schema.
5. Artifact preparation + contract promotion.
6. SageMaker pipeline upsert.
7. Initial materialization runs.
8. Step Functions manual deployment in AWS GUI (Build by code).
9. Readiness checks.
```

## Cell 3 (code)

```python
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

import boto3

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

UTC_NOW = datetime.now(timezone.utc).isoformat()
print('UTC now:', UTC_NOW)
```

## Cell 4 (code)

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
}
RUNTIME_PARAM_CONTRACTS = create_cfg.PIPELINE_RUNTIME_PARAMS
print('Pipeline builders:', len(PIPELINE_BUILDERS))
print('Runtime param contracts:', len(RUNTIME_PARAM_CONTRACTS))
print('Exception tables:', {k: v.default_table_name for k, v in EXCEPTION_TABLE_CONTRACTS.items()})
```

## Cell 5 (code)

```python
aws_region = 'us-east-1'
aws_account_id = ''  # e.g., 123456789012
project_name = 'ndr'
feature_spec_version = 'v3'
environment_name = 'dev'
owner = ''  # e.g., ml-platform-team

ml_project_name = ''  # e.g., ndr-ml-anomaly
ml_project_names = [ml_project_name] if ml_project_name else []

sagemaker_role_arn = ''  # e.g., arn:aws:iam::...:role/...
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

## Cell 6 (code)

```python
dpp_delta_root = ''
dpp_pair_counts_root = ''
dpp_fg_a_root = ''
dpp_fg_b_root = ''
dpp_fg_c_root = ''
dpp_machine_inventory_root = ''

mlp_predictions_root = ''
mlp_prediction_join_root = ''
mlp_publication_root = ''
mlp_training_reports_root = ''
mlp_training_artifacts_root = ''
mlp_production_model_root = ''
```

## Cell 7 (code)

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

## Cell 8 (code)

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
bootstrap_seed_items = _promote_pipeline_specs_ready(bootstrap_seed_items)
print('bootstrap items:', len(bootstrap_seed_items))
```

## Cell 9 (code)

```python
job_name_version = f'project_parameters#{feature_spec_version}'
dpp_s3_roots = {'delta_root': dpp_delta_root, 'pair_counts_root': dpp_pair_counts_root, 'fg_a_root': dpp_fg_a_root, 'fg_b_root': dpp_fg_b_root, 'fg_c_root': dpp_fg_c_root, 'machine_inventory_root': dpp_machine_inventory_root}
mlp_s3_roots = {'predictions_root': mlp_predictions_root, 'prediction_join_root': mlp_prediction_join_root, 'publication_root': mlp_publication_root, 'training_reports_root': mlp_training_reports_root, 'training_artifacts_root': mlp_training_artifacts_root, 'production_model_root': mlp_production_model_root}

dpp_reciprocal_item = {'project_name': project_name, 'job_name_version': job_name_version, 'ml_project_names': ml_project_names, 'updated_at': UTC_NOW, 'owner': owner, 'spec': {'project_name': project_name, 'ml_project_names': ml_project_names}}
dpp_s3_roots_item = {'project_name': project_name, 'job_name_version': 'S3_ROOTS', 'roots': dpp_s3_roots, 'updated_at': UTC_NOW, 'owner': owner}
mlp_reciprocal_item = {'ml_project_name': ml_project_name, 'job_name_version': job_name_version, 'project_name': project_name, 'updated_at': UTC_NOW, 'owner': owner, 'spec': {'project_name': project_name, 'ml_project_name': ml_project_name}}
mlp_s3_roots_item = {'ml_project_name': ml_project_name, 'job_name_version': 'S3_ROOTS', 'project_name': project_name, 'roots': mlp_s3_roots, 'updated_at': UTC_NOW, 'owner': owner}
```

## Cell 10 (code)

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

## Cell 11 (code)

```python
s3_schema_plan = {'bucket_name': default_bucket, 'prefixes': ['input/raw', 'input/reference', 'output/processed/delta_15m', 'output/processed/fg_a', 'output/processed/pair_counts', 'output/processed/fg_b', 'output/processed/fg_c', 'output/predictions', 'output/prediction_join', 'output/publication', 'artifacts/code', 'artifacts/pipeline', 'feature-store/offline', 'monitoring', 'logs']}
print(json.dumps(s3_schema_plan, indent=2))
```

## Cell 12 (code)

```python
def ensure_ddb_table(ddb_client, spec, dry_run=True):
    if dry_run:
        print('[DRY RUN] ensure table', spec['table_name']); return
    existing = set(ddb_client.list_tables()['TableNames'])
    if spec['table_name'] in existing: return
    ddb_client.create_table(TableName=spec['table_name'], BillingMode=spec['billing_mode'], KeySchema=spec['key_schema'], AttributeDefinitions=spec['attribute_definitions'])
    ddb_client.get_waiter('table_exists').wait(TableName=spec['table_name'])

def seed_table_items(ddb_resource, table_name, items, dry_run=True):
    if dry_run:
        print(f'[DRY RUN] seed {len(items)} items -> {table_name}'); return
    table = ddb_resource.Table(table_name)
    for item in items: table.put_item(Item=item)

def deploy_ddb(table_contracts, seed_plan, region_name, dry_run=True):
    ddb_client = boto3.client('dynamodb', region_name=region_name)
    ddb_resource = boto3.resource('dynamodb', region_name=region_name)
    for logical_name, contract in table_contracts.items():
        ensure_ddb_table(ddb_client, contract, dry_run=dry_run)
        seed_table_items(ddb_resource, contract['table_name'], seed_plan.get(logical_name, []), dry_run=dry_run)

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

def upsert_pipelines(project_name_for_contracts, feature_spec_version_for_contracts, role_arn, bucket, region_name, dry_run=True):
    for pipeline_job_name, builder in PIPELINE_BUILDERS.items():
        pipeline_name = f"{project_name_for_contracts}-{feature_spec_version_for_contracts}-{pipeline_job_name}"
        if dry_run:
            print('[DRY RUN] upsert pipeline', pipeline_name); continue
        pipeline = builder(pipeline_name=pipeline_name, role_arn=role_arn, default_bucket=bucket, region_name=region_name, project_name_for_contracts=project_name_for_contracts, feature_spec_version_for_contracts=feature_spec_version_for_contracts)
        pipeline.upsert(role_arn=role_arn)

def start_initial_materialization_runs(region_name, dry_run=True):
    if dry_run:
        print('[DRY RUN] would start initial feature/stats materialization runs')
        return
    print('Trigger pipeline starts here with finalized runtime parameters.')
```

## Cell 13 (code)

```python
artifact_commands = [
    f'python -m ndr.scripts.run_code_bundle_build --project-name {project_name} --feature-spec-version {feature_spec_version} --artifact-build-id {artifact_build_id}',
    f'python -m ndr.scripts.run_code_artifact_validate --project-name {project_name} --feature-spec-version {feature_spec_version} --artifact-build-id {artifact_build_id}',
    f'python -m ndr.scripts.run_code_smoke_validate --project-name {project_name} --feature-spec-version {feature_spec_version} --artifact-build-id {artifact_build_id}',
]
for i, cmd in enumerate(artifact_commands, 1): print(f'{i}. {cmd}')
print('code_artifact_s3_uri:', code_artifact_s3_uri or '<set me>')
print('expected build contract: code_bundle_build_output.v1')
print('expected validate contract: code_artifact_validate_report.v1')
print('expected smoke contract: code_smoke_validate_report.v1')
print('do not promote placeholders: artifact_build_id/artifact_sha256/code_artifact_s3_uri must be concrete values')
```

## Cell 14 (code)

```python
DRY_RUN = True
deploy_ddb(ddb_table_contracts, ddb_seed_items_plan, region_name=aws_region, dry_run=DRY_RUN)
ensure_s3_schema(s3_schema_plan, region_name=aws_region, dry_run=DRY_RUN)
upsert_pipelines(project_name_for_contracts=project_name, feature_spec_version_for_contracts=feature_spec_version, role_arn=sagemaker_role_arn, bucket=default_bucket, region_name=aws_region, dry_run=DRY_RUN)
start_initial_materialization_runs(region_name=aws_region, dry_run=DRY_RUN)
print('Dry-run deployment flow complete.')
```

## Cell 15 (markdown)

```markdown
## Step Functions deployment (manual)

Deploy state machines manually in AWS Step Functions GUI using Build by code and pasted JSON definitions.
```

## Cell 16 (code)

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
```

## Cell 17 (markdown)

```markdown
## Operator completion checklist

1. DDB tables created and seeded.
2. S3 schema created.
3. Artifact values set and promoted.
4. Pipelines upserted.
5. Initial feature/stats materialization runs executed.
6. Step Functions manually deployed.
```

## Notebook Metadata

```json
{
  "kernelspec": {
    "display_name": "Python 3",
    "language": "python",
    "name": "python3"
  },
  "language_info": {
    "name": "python",
    "version": "3.11"
  }
}
```

nbformat: `4`

nbformat_minor: `5`
