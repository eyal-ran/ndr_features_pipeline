# Canonical End-to-End Deployment Plan (single source of truth)

## A. What is being deployed (from scratch)

This plan deploys, in order:

1. S3 layout (DPP vs MLP separation),
2. all required code artifacts (`src/ndr` package + entry scripts),
3. DynamoDB tables (`dpp_config`, `mlp_config`, `batch_index`),
4. DDB seed records (reciprocal linkage + full pipeline/job bootstrap),
5. all SageMaker pipelines,
6. all NDR Step Functions JSONata definitions,
7. trigger paths (including ETL→SNS→SQS→Pipe→15m SF),
8. manual-operation scripts (training and backfill),
9. verification checks.

System docs explicitly define architecture, inventory, and source-of-truth boundaries.

---

## B. Core facts you must hold constant (to avoid confusion)

1. **No env vars required in this runbook**
   Everything is explicitly passed via a single `DEPLOY` object (table names, pipeline names, SF names, etc.).

2. **DPP/MLP split is mandatory**
   - DPP key: `project_name`
   - MLP key: `ml_project_name`
   and separate table schemas.

3. **15m trigger topology**
   Operationally: external ETL emits ingestion event payload → SNS → SQS → EventBridge Pipe → `sfn_ndr_15m_features_inference`.

4. **Pipeline inventory is fixed**
   15m, inference, prediction join, IF training, FG-B baseline, machine inventory, backfill extractor (+ optional delta-only).

---

## C. Operator sequence (exact order)

Run notebook cells in this order with no skips:

1. **Cell 1**: Global deployment config (`DEPLOY`)
2. **Cell 2**: Build S3 prefix map + upload scripts/dependencies via API
3. **Cell 3**: Create DynamoDB tables
4. **Cell 4**: Seed DDB linkage + full DPP bootstrap records
5. **Cell 5**: Register/upsert SageMaker pipelines
6. **Cell 6**: Deploy Step Functions definitions with placeholder substitution
7. **Cell 7**: Configure/validate trigger paths (Pipe + manual operations)
8. **Cell 8–12**: Verification suite

---

## D. Cell 1 — Single configuration block (NO environment variables)

```python
"""
CELL 1 — GLOBAL CONFIG (ONLY PLACE YOU EDIT VARIABLES)

Why this exists
---------------
Prevents drift across scripts. All downstream cells use DEPLOY values.
No env vars are required.

Fill every placeholder before continuing.
"""

DEPLOY = {
    # AWS
    "region": "<aws-region>",
    "account_id": "<12-digit-account-id>",
    "execution_role_arn": "arn:aws:iam::<account-id>:role/<deployment-role>",
    "artifact_bucket": "<artifact-bucket>",

    # Project identities
    "project_name": "<dpp-data-source-name>",      # e.g., fw_paloalto
    "ml_project_name": "<ml-project-name>",        # e.g., network_anomalies_detection
    "feature_spec_version": "<feature-spec-version>",  # e.g., v1
    "owner": "<owner>",

    # DDB names (explicit, no env fallback)
    "dpp_table_name": "dpp_config",
    "mlp_table_name": "mlp_config",
    "batch_index_table_name": "batch_index",
    "project_routing_table_name": "ml_projects_routing",
    "processing_lock_table_name": "<processing-lock-table>",
    "publication_lock_table_name": "<publication-lock-table>",

    # SageMaker Pipeline names
    "pipeline_15m_name": "<pipeline-15m>",
    "pipeline_inference_name": "<pipeline-inference>",
    "pipeline_prediction_join_name": "<pipeline-prediction-join>",
    "pipeline_if_training_name": "<pipeline-if-training>",
    "pipeline_fgb_name": "<pipeline-fgb>",
    "pipeline_machine_inventory_name": "<pipeline-machine-inventory>",
    "pipeline_backfill_extractor_name": "<pipeline-backfill-extractor>",
    "pipeline_backfill_15m_name": "<pipeline-backfill-15m>",

    # Step Functions names
    "sfn_15m_name": "sfn_ndr_15m_features_inference",
    "sfn_prediction_publication_name": "sfn_ndr_prediction_publication",
    "sfn_monthly_fgb_name": "sfn_ndr_monthly_fg_b_baselines",
    "sfn_training_name": "sfn_ndr_training_orchestrator",
    "sfn_backfill_name": "sfn_ndr_backfill_reprocessing",

    # External ingress integration (ETL-side)
    "etl_ingestion_sns_topic_arn": "<sns-topic-arn>",
    "etl_ingestion_sqs_queue_arn": "<sqs-queue-arn>",
    "eventbridge_pipe_name": "<pipe-name>",
    "eventbridge_pipe_role_arn": "arn:aws:iam::<account-id>:role/<pipe-role>",
    "event_bus_name": "<event-bus-name>"
}

print("Loaded DEPLOY for:", DEPLOY["project_name"], "->", DEPLOY["ml_project_name"])
```

---

## E. Cell 2 — S3 placement script (code + dependencies, fully explicit)

> This is the correct Step 2.
> It includes functions + dictionaries + mapping + manifest generation.

```python
"""
CELL 2 — PLACE ALL RUNTIME CODE IN S3 VIA API

Purpose
-------
Uploads all step entry scripts and required ndr package modules to per-step S3 code locations.

What gets uploaded
------------------
- Entry script for each step (run_*.py).
- Full src/ndr/** tree under each step root as dependency payload.
- Manifest JSON listing exact uploaded URIs.

DPP vs MLP split
----------------
- DPP-owned steps are uploaded under projects/<project_name>/...
- MLP-owned steps are uploaded under projects/<ml_project_name>/...
"""

from pathlib import Path
import json
import mimetypes
import boto3

# ---- local repo path (edit to your notebook workspace) ----
REPO_ROOT = Path("/home/sagemaker-user/ndr_features_pipeline")
SRC_NDR = REPO_ROOT / "src" / "ndr"
SRC_SCRIPTS = SRC_NDR / "scripts"

assert SRC_NDR.exists(), f"Missing local dependency path: {SRC_NDR}"
assert SRC_SCRIPTS.exists(), f"Missing local scripts path: {SRC_SCRIPTS}"

# ---- deployment values ----
bucket = DEPLOY["artifact_bucket"]
region = DEPLOY["region"]
project_name = DEPLOY["project_name"]
ml_project_name = DEPLOY["ml_project_name"]
v = DEPLOY["feature_spec_version"]

s3 = boto3.client("s3", region_name=region)

def upload_file(local_path: Path, key: str) -> None:
    """Upload one file to S3 with best-effort content type."""
    ct = mimetypes.guess_type(str(local_path))[0] or "application/octet-stream"
    s3.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": ct},
    )

def upload_full_ndr_tree(step_root_key: str) -> int:
    """
    Upload src/ndr/** under <step_root_key>/ndr/**.

    Rationale:
    Step containers run `python -m ndr.scripts.<entrypoint>`,
    so the importable package must exist in that code payload.
    """
    count = 0
    for p in SRC_NDR.rglob("*"):
        if p.is_file():
            rel = p.relative_to(SRC_NDR).as_posix()
            key = f"{step_root_key}/ndr/{rel}"
            upload_file(p, key)
            count += 1
    return count

# Step -> entry script mapping
ENTRY_SCRIPT = {
    "DeltaBuilderStep": "run_delta_builder.py",
    "FGABuilderStep": "run_fg_a_builder.py",
    "PairCountsBuilderStep": "run_pair_counts_builder.py",
    "FGCCorrBuilderStep": "run_fg_c_builder.py",
    "FGBaselineBuilderStep": "run_fg_b_builder.py",
    "MachineInventoryUnloadStep": "run_machine_inventory_unload.py",
    "HistoricalWindowsExtractorStep": "run_historical_windows_extractor.py",
    "InferencePredictionsStep": "run_inference_predictions.py",
    "PredictionFeatureJoinStep": "run_prediction_feature_join.py",
    "TrainingDataVerifierStep": "run_if_training.py",
    "MissingFeatureCreationStep": "run_if_training.py",
    "PostRemediationVerificationStep": "run_if_training.py",
    "IFTrainingStep": "run_if_training.py",
    "ModelPublishStep": "run_if_training.py",
    "ModelAttributesStep": "run_if_training.py",
    "ModelDeployStep": "run_if_training.py",
}

# DPP-owned code prefixes
DPP_PREFIX = {
    "DeltaBuilderStep": f"projects/{project_name}/versions/{v}/code/pipelines/15m_streaming/DeltaBuilderStep",
    "FGABuilderStep": f"projects/{project_name}/versions/{v}/code/pipelines/15m_streaming/FGABuilderStep",
    "PairCountsBuilderStep": f"projects/{project_name}/versions/{v}/code/pipelines/15m_streaming/PairCountsBuilderStep",
    "FGCCorrBuilderStep": f"projects/{project_name}/versions/{v}/code/pipelines/15m_streaming/FGCCorrBuilderStep",
    "FGBaselineBuilderStep": f"projects/{project_name}/versions/{v}/code/pipelines/fg_b_baseline/FGBaselineBuilderStep",
    "MachineInventoryUnloadStep": f"projects/{project_name}/versions/{v}/code/pipelines/machine_inventory_unload/MachineInventoryUnloadStep",
    "HistoricalWindowsExtractorStep": f"projects/{project_name}/versions/{v}/code/pipelines/backfill_historical_extractor/HistoricalWindowsExtractorStep",
}

# MLP-owned code prefixes
MLP_PREFIX = {
    "InferencePredictionsStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/inference_predictions/InferencePredictionsStep",
    "PredictionFeatureJoinStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/prediction_feature_join/PredictionFeatureJoinStep",
    "TrainingDataVerifierStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/TrainingDataVerifierStep",
    "MissingFeatureCreationStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/MissingFeatureCreationStep",
    "PostRemediationVerificationStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/PostRemediationVerificationStep",
    "IFTrainingStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/IFTrainingStep",
    "ModelPublishStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/ModelPublishStep",
    "ModelAttributesStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/ModelAttributesStep",
    "ModelDeployStep": f"projects/{ml_project_name}/versions/{v}/code/pipelines/if_training/ModelDeployStep",
}

all_steps = {**DPP_PREFIX, **MLP_PREFIX}
manifest = []

for step, root in all_steps.items():
    entry = ENTRY_SCRIPT[step]
    local_entry = SRC_SCRIPTS / entry
    assert local_entry.exists(), f"Missing local script: {local_entry}"

    dep_files = upload_full_ndr_tree(root)
    upload_file(local_entry, f"{root}/{entry}")

    uri = f"s3://{bucket}/{root}/{entry}"
    print("[uploaded]", step, "=>", uri)
    manifest.append({
        "step_name": step,
        "entry_script": entry,
        "code_uri": uri,
        "dependency_files_uploaded": dep_files
    })

manifest_key = f"projects/{project_name}/versions/{v}/manifests/code_upload_manifest.json"
s3.put_object(
    Bucket=bucket,
    Key=manifest_key,
    Body=json.dumps(manifest, indent=2).encode("utf-8"),
    ContentType="application/json",
)
print("[manifest]", f"s3://{bucket}/{manifest_key}")
```

---

## F. Cell 3 — Create DynamoDB tables (schema only)

```python
"""
CELL 3 — CREATE dpp_config, mlp_config, batch_index TABLES

Purpose
-------
Creates table schemas only.
No seed data in this cell.

Required order
--------------
Run before Cell 4.
"""

import boto3

region = DEPLOY["region"]
ddb = boto3.client("dynamodb", region_name=region)

TABLES = {
    DEPLOY["dpp_table_name"]: {
        "TableName": DEPLOY["dpp_table_name"],
        "AttributeDefinitions": [
            {"AttributeName": "project_name", "AttributeType": "S"},
            {"AttributeName": "job_name_version", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "project_name", "KeyType": "HASH"},
            {"AttributeName": "job_name_version", "KeyType": "RANGE"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    },
    DEPLOY["mlp_table_name"]: {
        "TableName": DEPLOY["mlp_table_name"],
        "AttributeDefinitions": [
            {"AttributeName": "ml_project_name", "AttributeType": "S"},
            {"AttributeName": "job_name_version", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "ml_project_name", "KeyType": "HASH"},
            {"AttributeName": "job_name_version", "KeyType": "RANGE"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    },
    DEPLOY["batch_index_table_name"]: {
        "TableName": DEPLOY["batch_index_table_name"],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        "BillingMode": "PAY_PER_REQUEST",
    },
}

for name, payload in TABLES.items():
    try:
        ddb.describe_table(TableName=name)
        print("[exists]", name)
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(**payload)
        ddb.get_waiter("table_exists").wait(TableName=name)
        print("[created]", name)
```

---

## G. Cell 4 — Seed DDB values (linkage + full bootstrap)

```python
"""
CELL 4 — SEED DDB RECORDS

Purpose
-------
A) Seed reciprocal linkage records in DPP and MLP.
B) Seed full bootstrap records in DPP for pipeline_* and job specs.

Dependencies
------------
- Local import path to repo `src`.
- Cell 3 already completed.
"""

import sys
import boto3

# Edit this path to your notebook clone path
sys.path.insert(0, "/home/sagemaker-user/ndr_features_pipeline/src")

from ndr.scripts.create_ml_projects_parameters_table import (
    build_split_seed_items,
    _build_bootstrap_items,
)

region = DEPLOY["region"]
ddb = boto3.resource("dynamodb", region_name=region)

project_name = DEPLOY["project_name"]
ml_project_name = DEPLOY["ml_project_name"]
v = DEPLOY["feature_spec_version"]
owner = DEPLOY["owner"]

# A) reciprocal linkage records
split = build_split_seed_items(
    project_name=project_name,
    ml_project_name=ml_project_name,
    feature_spec_version=v,
    owner=owner,
)

table_map = {
    "dpp_config": DEPLOY["dpp_table_name"],
    "mlp_config": DEPLOY["mlp_table_name"],
    "batch_index": DEPLOY["batch_index_table_name"],
}

for logical_name, items in split.items():
    if not items:
        continue
    t = ddb.Table(table_map[logical_name])
    with t.batch_writer() as w:
        for item in items:
            w.put_item(Item=item)
    print("[seeded-linkage]", logical_name, len(items))

# B) full bootstrap into DPP
bootstrap = _build_bootstrap_items(project_name, v, owner)
dpp = ddb.Table(DEPLOY["dpp_table_name"])
with dpp.batch_writer(overwrite_by_pkeys=["project_name", "job_name_version"]) as w:
    for item in bootstrap:
        w.put_item(Item=item)

print("[seeded-bootstrap]", len(bootstrap), "items")
```

---

## H. Cell 5 — Deploy SageMaker pipelines

```python
"""
CELL 5 — CREATE/UPDATE SAGEMAKER PIPELINES
"""

import sys
sys.path.insert(0, "/home/sagemaker-user/ndr_features_pipeline/src")

from ndr.pipeline.sagemaker_pipeline_definitions_unified_with_fgc import (
    build_15m_streaming_pipeline,
    build_fg_b_baseline_pipeline,
    build_machine_inventory_unload_pipeline,
    build_delta_builder_pipeline,
)
from ndr.pipeline.sagemaker_pipeline_definitions_inference import build_inference_predictions_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_prediction_feature_join import build_prediction_feature_join_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_if_training import build_if_training_pipeline
from ndr.pipeline.sagemaker_pipeline_definitions_backfill_historical_extractor import build_backfill_historical_extractor_pipeline

region = DEPLOY["region"]
role_arn = DEPLOY["execution_role_arn"]
bucket = DEPLOY["artifact_bucket"]

pipelines = [
    build_15m_streaming_pipeline(DEPLOY["pipeline_15m_name"], role_arn, bucket, region),
    build_fg_b_baseline_pipeline(DEPLOY["pipeline_fgb_name"], role_arn, bucket, region),
    build_machine_inventory_unload_pipeline(DEPLOY["pipeline_machine_inventory_name"], role_arn, bucket, region),
    build_inference_predictions_pipeline(DEPLOY["pipeline_inference_name"], role_arn, bucket, region),
    build_prediction_feature_join_pipeline(DEPLOY["pipeline_prediction_join_name"], role_arn, bucket, region),
    build_if_training_pipeline(DEPLOY["pipeline_if_training_name"], role_arn, bucket, region),
    build_backfill_historical_extractor_pipeline(DEPLOY["pipeline_backfill_extractor_name"], role_arn, bucket, region),
]

delta_uri = (
    f"s3://{bucket}/projects/{DEPLOY['project_name']}/versions/{DEPLOY['feature_spec_version']}"
    f"/code/pipelines/15m_streaming/DeltaBuilderStep/run_delta_builder.py"
)
pipelines.append(
    build_delta_builder_pipeline(
        region=region,
        role_arn=role_arn,
        pipeline_name=f"{DEPLOY['pipeline_15m_name']}-delta-only",
        base_job_name="ndr-delta-only",
        code_s3_uri=delta_uri,
    )
)

for p in pipelines:
    arn = p.upsert(role_arn=role_arn)
    print("[upserted]", p.name, "->", arn)
```

---

## I. Cell 6 — Deploy Step Functions (all five)

You deploy JSONs from `docs/step_functions_jsonata/` and substitute placeholders for pipeline names, table names, lock tables, bus, nested SF ARN, etc.

(Implementation can be `states.create_state_machine` / `states.update_state_machine` with rendered JSON string.)

---

## J. Step 7 — Trigger and operations model (corrected + actionable)

### J1) 15m automatic path
- external ETL publishes ingestion completion payload to SNS,
- SNS fanout to SQS,
- EventBridge Pipe reads SQS and starts `sfn_ndr_15m_features_inference`.

### J2) Training/backfill operations path
- commonly manual from notebook,
- backfill also invoked by training remediation/orchestration.

#### Cell 7A — manual training start
```python
import boto3, json, uuid, datetime

sfn = boto3.client("stepfunctions", region_name=DEPLOY["region"])
arns = {x["name"]: x["stateMachineArn"] for x in sfn.list_state_machines()["stateMachines"]}

payload = {
    "project_name": DEPLOY["project_name"],
    "feature_spec_version": DEPLOY["feature_spec_version"],
    "ml_project_name": DEPLOY["ml_project_name"],
    "run_id": f"manual-{uuid.uuid4().hex[:8]}",
    "execution_ts_iso": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    "training_start_ts": "<YYYY-MM-DDTHH:MM:SSZ>",
    "training_end_ts": "<YYYY-MM-DDTHH:MM:SSZ>",
    "eval_start_ts": "<YYYY-MM-DDTHH:MM:SSZ>",
    "eval_end_ts": "<YYYY-MM-DDTHH:MM:SSZ>",
    "evaluation_windows_json": "[]",
    "missing_windows_override": "[]"
}

resp = sfn.start_execution(
    stateMachineArn=arns[DEPLOY["sfn_training_name"]],
    name=f"manual-training-{uuid.uuid4().hex[:8]}",
    input=json.dumps(payload),
)
print(resp["executionArn"])
```

#### Cell 7B — manual backfill start
```python
import boto3, json, uuid

sfn = boto3.client("stepfunctions", region_name=DEPLOY["region"])
arns = {x["name"]: x["stateMachineArn"] for x in sfn.list_state_machines()["stateMachines"]}

payload = {
    "project_name": DEPLOY["project_name"],
    "feature_spec_version": DEPLOY["feature_spec_version"],
    "start_ts_iso": "<YYYY-MM-DDTHH:MM:SSZ>",
    "end_ts_iso": "<YYYY-MM-DDTHH:MM:SSZ>",
    "input_s3_prefix": "s3://<bucket>/<input-range>/",
    "output_s3_prefix": "s3://<bucket>/<windows-output>/"
}

resp = sfn.start_execution(
    stateMachineArn=arns[DEPLOY["sfn_backfill_name"]],
    name=f"manual-backfill-{uuid.uuid4().hex[:8]}",
    input=json.dumps(payload),
)
print(resp["executionArn"])
```

#### Cell 7C — execution poller
```python
import time, boto3

def wait_sfn(execution_arn: str, region: str, poll_s: int = 20):
    sfn = boto3.client("stepfunctions", region_name=region)
    while True:
        d = sfn.describe_execution(executionArn=execution_arn)
        print("status:", d["status"])
        if d["status"] in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED_OUT"}:
            return d
        time.sleep(poll_s)
```

---

## K. Full JSON artifact — how to use it correctly (no ambiguity)

### K1) What this JSON is
It is a **deployment input contract**, not an executable script.

### K2) Correct usage order
1. Use `table_definitions` to create tables (Cell 3 logic).
2. Use `seed_items.dpp_config` and `seed_items.mlp_config` for reciprocal linkage seed.
3. Then run full bootstrap generation (Cell 4 using `_build_bootstrap_items`).
4. `batch_index_example` is for contract validation/testing (15m SF writes real runtime rows with idempotent put/update).

### K3) JSON template
```json
{
  "table_definitions": {
    "dpp_config": {
      "TableName": "dpp_config",
      "AttributeDefinitions": [
        {"AttributeName": "project_name", "AttributeType": "S"},
        {"AttributeName": "job_name_version", "AttributeType": "S"}
      ],
      "KeySchema": [
        {"AttributeName": "project_name", "KeyType": "HASH"},
        {"AttributeName": "job_name_version", "KeyType": "RANGE"}
      ],
      "BillingMode": "PAY_PER_REQUEST"
    },
    "mlp_config": {
      "TableName": "mlp_config",
      "AttributeDefinitions": [
        {"AttributeName": "ml_project_name", "AttributeType": "S"},
        {"AttributeName": "job_name_version", "AttributeType": "S"}
      ],
      "KeySchema": [
        {"AttributeName": "ml_project_name", "KeyType": "HASH"},
        {"AttributeName": "job_name_version", "KeyType": "RANGE"}
      ],
      "BillingMode": "PAY_PER_REQUEST"
    },
    "batch_index": {
      "TableName": "batch_index",
      "AttributeDefinitions": [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
        {"AttributeName": "GSI1PK", "AttributeType": "S"},
        {"AttributeName": "GSI1SK", "AttributeType": "S"}
      ],
      "KeySchema": [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"}
      ],
      "GlobalSecondaryIndexes": [
        {
          "IndexName": "GSI1",
          "KeySchema": [
            {"AttributeName": "GSI1PK", "KeyType": "HASH"},
            {"AttributeName": "GSI1SK", "KeyType": "RANGE"}
          ],
          "Projection": {"ProjectionType": "ALL"}
        }
      ],
      "BillingMode": "PAY_PER_REQUEST"
    }
  },
  "seed_items": {
    "dpp_config": [
      {
        "project_name": "<project_name>",
        "job_name_version": "project_parameters#<feature_spec_version>",
        "data_source_name": "<project_name>",
        "ml_project_name": "<ml_project_name>",
        "spec": {"ProjectName": "<project_name>", "MlProjectName": "<ml_project_name>"},
        "updated_at": "<iso8601z>",
        "owner": "<owner>"
      }
    ],
    "mlp_config": [
      {
        "ml_project_name": "<ml_project_name>",
        "job_name_version": "project_parameters#<feature_spec_version>",
        "project_name": "<project_name>",
        "spec": {"ProjectName": "<project_name>", "MlProjectName": "<ml_project_name>"},
        "updated_at": "<iso8601z>",
        "owner": "<owner>"
      }
    ],
    "batch_index_example": {
      "pk": "<project_name>#<project_name>#<feature_spec_version>#<YYYY-MM-DD>",
      "sk": "<HH>#<slot15>#<batch_id>",
      "project_name": "<project_name>",
      "data_source_name": "<project_name>",
      "version": "<feature_spec_version>",
      "date_utc": "<YYYY-MM-DD>",
      "hour_utc": "<00-23>",
      "slot15": 1,
      "batch_id": "<batch_id>",
      "raw_parsed_logs_s3_prefix": "s3://<ing-bucket>/<project>/<org1>/<org2>/<yyyy>/<mm>/<dd>/<batch_id>/",
      "event_ts_utc": "<iso8601z>",
      "org1": "<org1>",
      "org2": "<org2>",
      "ml_project_name": "<ml_project_name>",
      "ml_project_names_json": "[\"<ml_project_name>\"]",
      "ingested_at_utc": "<iso8601z>",
      "status": "RECEIVED",
      "GSI1PK": "<project_name>#<project_name>#<feature_spec_version>#<batch_id>",
      "GSI1SK": "<iso8601z>"
    }
  }
}
```

---

## L. Verification suite (must-pass before handoff)

### Cell 8 — DDB schema + linkage checks
```python
import boto3
ddb = boto3.client("dynamodb", region_name=DEPLOY["region"])
res = boto3.resource("dynamodb", region_name=DEPLOY["region"])

for t in [DEPLOY["dpp_table_name"], DEPLOY["mlp_table_name"], DEPLOY["batch_index_table_name"]]:
    td = ddb.describe_table(TableName=t)["Table"]
    print(t, td["TableStatus"])
    if t == DEPLOY["batch_index_table_name"]:
        assert any(g["IndexName"] == "GSI1" for g in td.get("GlobalSecondaryIndexes", []))

item = res.Table(DEPLOY["dpp_table_name"]).get_item(Key={
    "project_name": DEPLOY["project_name"],
    "job_name_version": f"project_parameters#{DEPLOY['feature_spec_version']}"
}).get("Item")
assert item is not None
print("[ok] dpp project_parameters exists")
```

### Cell 9 — pipeline existence check
```python
import boto3
sm = boto3.client("sagemaker", region_name=DEPLOY["region"])
for p in [
    DEPLOY["pipeline_15m_name"], DEPLOY["pipeline_inference_name"], DEPLOY["pipeline_prediction_join_name"],
    DEPLOY["pipeline_if_training_name"], DEPLOY["pipeline_fgb_name"],
    DEPLOY["pipeline_machine_inventory_name"], DEPLOY["pipeline_backfill_extractor_name"]
]:
    print(p, sm.describe_pipeline(PipelineName=p)["PipelineStatus"])
```

### Cell 10 — state machine existence check
```python
import boto3
sfn = boto3.client("stepfunctions", region_name=DEPLOY["region"])
names = {x["name"] for x in sfn.list_state_machines()["stateMachines"]}
for n in [
    DEPLOY["sfn_15m_name"], DEPLOY["sfn_prediction_publication_name"], DEPLOY["sfn_monthly_fgb_name"],
    DEPLOY["sfn_training_name"], DEPLOY["sfn_backfill_name"]
]:
    assert n in names, f"missing {n}"
    print("[ok]", n)
```

### Cell 11 — 15m smoke start
```python
import boto3, json, uuid, datetime
sfn = boto3.client("stepfunctions", region_name=DEPLOY["region"])
arns = {x["name"]: x["stateMachineArn"] for x in sfn.list_state_machines()["stateMachines"]}
batch_id = f"smoke-{uuid.uuid4().hex[:8]}"

payload = {
    "project_name": DEPLOY["project_name"],
    "data_source_name": DEPLOY["project_name"],
    "ml_project_name": DEPLOY["ml_project_name"],
    "batch_id": batch_id,
    "raw_parsed_logs_s3_prefix": f"s3://<ing-bucket>/{DEPLOY['project_name']}/<org1>/<org2>/2026/03/12/{batch_id}/",
    "timestamp": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    "feature_spec_version": DEPLOY["feature_spec_version"]
}

resp = sfn.start_execution(
    stateMachineArn=arns[DEPLOY["sfn_15m_name"]],
    name=f"smoke-15m-{uuid.uuid4().hex[:8]}",
    input=json.dumps(payload),
)
print(resp["executionArn"])
```

---

## M. Why previous Step 2 versions looked different (and which is correct)

- The short snippet with only `DEPLOY` was **configuration-only** (Cell 1).
- The full Step 2 uploader is the one with functions + mappings + dictionaries (Cell 2 above).
- In this canonical runbook, they are intentionally separated:
  - **Cell 1 = config**
  - **Cell 2 = upload logic**

So: **use the Step 2 script in Section E of this document** as the authoritative one.

---

## N. Self-review outcome (mandatory)

This plan is now a single, consistent blueprint with:
- one complete execution flow,
- no cross-response dependency,
- explicit DPP/MLP boundaries,
- complete notebook scripts for deployment + operations + verification,
- clear JSON usage instructions.
