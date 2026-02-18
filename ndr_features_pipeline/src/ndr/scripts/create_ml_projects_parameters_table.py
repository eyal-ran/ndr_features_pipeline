"""Create and seed the ML project parameters DynamoDB table.

This module supports two invocation patterns for SageMaker JupyterLab users:

1) CLI execution from a terminal cell:

   .. code-block:: bash

      PYTHONPATH=src python -m ndr.scripts.create_ml_projects_parameters_table \
          --project-name ndr-prod \
          --feature-spec-version v1 \
          --owner ndr-team \
          --region us-east-1

2) Direct notebook invocation from Python code:

   .. code-block:: python

      from ndr.scripts.create_ml_projects_parameters_table import run_from_notebook

      summary = run_from_notebook(
          {
              "project_name": "ndr-prod",
              "feature_spec_version": "v1",
              "owner": "ndr-team",
              "region_name": "us-east-1",
              # Optional:
              # "table_name": "ml_projects_parameters",
              # "use_json_table_definition": True,
          }
      )
      summary

The implementation follows ``docs/DYNAMODB_PROJECT_PARAMETERS_SPEC.md``:

- DynamoDB table name default: ``ml_projects_parameters``.
- Table-name env var precedence:
  1. explicit argument
  2. ``ML_PROJECTS_PARAMETERS_TABLE_NAME``
  3. ``JOB_SPEC_DDB_TABLE_NAME`` (legacy)
  4. default constant
- Composite primary key:
  - HASH ``project_name`` (string)
  - RANGE ``job_name`` (string)
- Sort-key value convention: ``<job_name>#<feature_spec_version>``.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

DDB_TABLE_ENV_VAR = "ML_PROJECTS_PARAMETERS_TABLE_NAME"
LEGACY_DDB_TABLE_ENV_VAR = "JOB_SPEC_DDB_TABLE_NAME"
DEFAULT_TABLE_NAME = "ml_projects_parameters"
DEFAULT_ROUTING_TABLE_NAME = "ml_projects_routing"
JOB_SPEC_SORT_KEY_DELIMITER = "#"

TABLE_SPEC_JSON: dict[str, Any] = {
    "table_metadata": {
        "purpose": (
            "Single source of truth for project-specific configuration used by the "
            "NDR feature engineering pipeline."
        ),
        "environment_variables": [
            DDB_TABLE_ENV_VAR,
            LEGACY_DDB_TABLE_ENV_VAR,
        ],
        "table_name_example": DEFAULT_TABLE_NAME,
        "key_convention": {
            "partition_key": "project_name",
            "sort_key": "job_name",
            "versioned_sort_key_format": "<job_name>#<feature_spec_version>",
            "sort_key_delimiter": JOB_SPEC_SORT_KEY_DELIMITER,
        },
        "required_attributes": [
            "project_name",
            "job_name",
            "spec",
        ],
        "optional_attributes": [
            "feature_spec_version",
            "updated_at",
            "owner",
            "pipeline_defaults",
            "feature_store",
            "tags",
        ],
    },
    "create_table_request": {
        "TableName": DEFAULT_TABLE_NAME,
        "AttributeDefinitions": [
            {"AttributeName": "project_name", "AttributeType": "S"},
            {"AttributeName": "job_name", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "project_name", "KeyType": "HASH"},
            {"AttributeName": "job_name", "KeyType": "RANGE"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    },
}

NDR_SEED_PROFILE = "ndr"
NO_SEED_PROFILE = "none"
CUSTOM_JSON_SEED_PROFILE = "custom-json"
VALID_SEED_PROFILES = (NDR_SEED_PROFILE, NO_SEED_PROFILE, CUSTOM_JSON_SEED_PROFILE)

PIPELINE_RUNTIME_PARAMS = {
    "pipeline_15m_streaming": [
        "ProjectName",
        "FeatureSpecVersion",
        "MiniBatchId",
        "BatchStartTsIso",
        "BatchEndTsIso",
    ],
    "pipeline_fg_b_baseline": [
        "ProjectName",
        "FeatureSpecVersion",
        "ReferenceTimeIso",
        "Mode",
    ],
    "pipeline_machine_inventory_unload": [
        "ProjectName",
        "FeatureSpecVersion",
        "ReferenceMonthIso",
    ],
    "pipeline_inference_predictions": [
        "ProjectName",
        "FeatureSpecVersion",
        "MiniBatchId",
        "BatchStartTsIso",
        "BatchEndTsIso",
    ],
    "pipeline_prediction_feature_join": [
        "ProjectName",
        "FeatureSpecVersion",
        "MiniBatchId",
        "BatchStartTsIso",
        "BatchEndTsIso",
    ],
    "pipeline_if_training": [
        "ProjectName",
        "FeatureSpecVersion",
        "RunId",
        "ExecutionTsIso",
    ],
}

REQUIRED_TABLE_CREATE_KEYS = (
    "TableName",
    "AttributeDefinitions",
    "KeySchema",
    "BillingMode",
)


def resolve_routing_table_name(explicit_table_name: str | None = None) -> str:
    """Resolve DynamoDB routing table name.

    Resolution precedence:
    1. explicit argument
    2. ``ML_PROJECTS_ROUTING_TABLE_NAME``
    3. default constant
    """
    return explicit_table_name or os.getenv("ML_PROJECTS_ROUTING_TABLE_NAME") or DEFAULT_ROUTING_TABLE_NAME


def _utc_now_iso8601() -> str:
    """Return current UTC timestamp formatted as ISO8601 with ``Z`` suffix."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_table_name(explicit_table_name: str | None = None) -> str:
    """Resolve DynamoDB table name using the documented precedence.

    Parameters
    ----------
    explicit_table_name:
        Table name explicitly supplied by caller.

    Returns
    -------
    str
        Resolved table name.
    """
    return (
        explicit_table_name
        or os.getenv(DDB_TABLE_ENV_VAR)
        or os.getenv(LEGACY_DDB_TABLE_ENV_VAR)
        or DEFAULT_TABLE_NAME
    )


def _build_create_table_payload(table_name: str, use_json_table_definition: bool) -> dict[str, Any]:
    """Build payload for ``dynamodb.create_table``.

    Parameters
    ----------
    table_name:
        Concrete table name to create.
    use_json_table_definition:
        When ``True``, base request fields are copied from ``TABLE_SPEC_JSON``.
        When ``False``, request is built from code constants.

    Returns
    -------
    dict[str, Any]
        ``create_table`` kwargs payload.
    """
    if use_json_table_definition:
        payload = dict(TABLE_SPEC_JSON["create_table_request"])
    else:
        payload = {
            "AttributeDefinitions": [
                {"AttributeName": "project_name", "AttributeType": "S"},
                {"AttributeName": "job_name", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "project_name", "KeyType": "HASH"},
                {"AttributeName": "job_name", "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }

    payload["TableName"] = table_name
    return payload


def _validate_create_table_payload(payload: dict[str, Any]) -> None:
    """Validate DynamoDB create-table payload against required schema fields.

    This catches malformed payloads early and provides deterministic errors
    before making AWS API calls.
    """
    missing = [key for key in REQUIRED_TABLE_CREATE_KEYS if key not in payload]
    if missing:
        raise ValueError(f"Create table payload missing required keys: {missing}")

    billing_mode = payload.get("BillingMode")
    if billing_mode != "PAY_PER_REQUEST":
        raise ValueError(
            "BillingMode must be PAY_PER_REQUEST to match specification; "
            f"got {billing_mode!r}"
        )

    attribute_definitions = payload.get("AttributeDefinitions")
    if not isinstance(attribute_definitions, list):
        raise ValueError("AttributeDefinitions must be a list")

    key_schema = payload.get("KeySchema")
    if not isinstance(key_schema, list):
        raise ValueError("KeySchema must be a list")

    attributes = {
        item.get("AttributeName"): item.get("AttributeType")
        for item in attribute_definitions
        if isinstance(item, dict)
    }
    if attributes.get("project_name") != "S" or attributes.get("job_name") != "S":
        raise ValueError(
            "AttributeDefinitions must include project_name(S) and job_name(S)"
        )

    key_types = {
        item.get("AttributeName"): item.get("KeyType")
        for item in key_schema
        if isinstance(item, dict)
    }
    if key_types.get("project_name") != "HASH" or key_types.get("job_name") != "RANGE":
        raise ValueError(
            "KeySchema must include project_name(HASH) and job_name(RANGE)"
        )


def _build_routing_table_payload(table_name: str) -> dict[str, Any]:
    """Build payload for routing table creation (org_key HASH)."""
    return {
        "TableName": table_name,
        "AttributeDefinitions": [
            {"AttributeName": "org_key", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "org_key", "KeyType": "HASH"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }


def create_routing_table_if_missing(
    table_name: str,
    region_name: str | None = None,
) -> dict[str, Any]:
    """Create routing table if absent and wait until it exists."""
    ddb_client = boto3.client("dynamodb", region_name=region_name)

    try:
        return ddb_client.describe_table(TableName=table_name)["Table"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise

    ddb_client.create_table(**_build_routing_table_payload(table_name))
    waiter = ddb_client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)
    return ddb_client.describe_table(TableName=table_name)["Table"]


def upsert_routing_items(
    table_name: str,
    items: list[dict[str, Any]],
    region_name: str | None = None,
) -> int:
    """Validate and upsert routing records into the routing table."""
    if not items:
        return 0

    table = boto3.resource("dynamodb", region_name=region_name).Table(table_name)
    required_item_keys = {"org_key", "project_name", "ingestion_prefix"}

    with table.batch_writer(overwrite_by_pkeys=["org_key"]) as writer:
        for item in items:
            missing = required_item_keys - set(item.keys())
            if missing:
                raise ValueError(f"Routing item missing required keys {sorted(missing)}: {item}")
            writer.put_item(Item=item)

    return len(items)


def create_table_if_missing(
    table_name: str,
    region_name: str | None = None,
    use_json_table_definition: bool = False,
) -> dict[str, Any]:
    """Create table if absent and wait until it exists.

    Parameters
    ----------
    table_name:
        Target DynamoDB table name.
    region_name:
        Optional AWS region override.
    use_json_table_definition:
        If ``True``, create request is sourced from ``TABLE_SPEC_JSON``.

    Returns
    -------
    dict[str, Any]
        DynamoDB table description.
    """
    ddb_client = boto3.client("dynamodb", region_name=region_name)

    try:
        return ddb_client.describe_table(TableName=table_name)["Table"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise

    create_payload = _build_create_table_payload(
        table_name=table_name,
        use_json_table_definition=use_json_table_definition,
    )
    _validate_create_table_payload(create_payload)
    ddb_client.create_table(**create_payload)

    waiter = ddb_client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)
    return ddb_client.describe_table(TableName=table_name)["Table"]


def _versioned_job_name(job_name: str, feature_spec_version: str) -> str:
    """Return the standardized versioned sort-key value for a job spec item."""
    return f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}{feature_spec_version}"


def _build_bootstrap_items(
    project_name: str,
    feature_spec_version: str,
    owner: str,
) -> list[dict[str, Any]]:
    """Build starter records with spec placeholders for current pipelines.

    Parameters
    ----------
    project_name:
        Partition-key value used for all seeded job specs.
    feature_spec_version:
        Version suffix used in sort keys and relevant payload fields.
    owner:
        Owner metadata value recorded per item.

    Returns
    -------
    list[dict[str, Any]]
        List of seed items ready for DynamoDB ``put_item`` writes.
    """
    now = _utc_now_iso8601()

    pipeline_items = [
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pipeline_15m_streaming", feature_spec_version),
            "spec": {
                "required_runtime_params": PIPELINE_RUNTIME_PARAMS["pipeline_15m_streaming"],
                "scripts": {
                    "steps": {
                        "DeltaBuilderStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/15m_streaming/DeltaBuilderStep/",
                            "entry_script": "run_delta_builder.py",
                            "data_prefixes": {
                                "input_traffic": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/raw/traffic/",
                                "output_delta": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/delta/",
                            },
                        },
                        "FGABuilderStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/15m_streaming/FGABuilderStep/",
                            "entry_script": "run_fg_a_builder.py",
                            "data_prefixes": {
                                "input_delta": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/delta/",
                                "output_fg_a": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_a/",
                            },
                        },
                        "PairCountsBuilderStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/15m_streaming/PairCountsBuilderStep/",
                            "entry_script": "run_pair_counts_builder.py",
                            "data_prefixes": {
                                "input_traffic": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/raw/traffic/",
                                "output_pair_counts": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/pair_counts/",
                            },
                        },
                        "FGCCorrBuilderStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/15m_streaming/FGCCorrBuilderStep/",
                            "entry_script": "run_fg_c_builder.py",
                            "data_prefixes": {
                                "input_fg_a": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_a/",
                                "input_fg_b": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_b/",
                                "output_fg_c": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_c/",
                            },
                        },
                    }
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pipeline_fg_b_baseline", feature_spec_version),
            "spec": {
                "required_runtime_params": PIPELINE_RUNTIME_PARAMS["pipeline_fg_b_baseline"],
                "scripts": {
                    "steps": {
                        "FGBaselineBuilderStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/fg_b_baseline/FGBaselineBuilderStep/",
                            "entry_script": "run_fg_b_builder.py",
                            "data_prefixes": {
                                "input_fg_a": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_a/",
                                "output_fg_b": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_b/",
                            },
                        }
                    }
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pipeline_machine_inventory_unload", feature_spec_version),
            "spec": {
                "required_runtime_params": PIPELINE_RUNTIME_PARAMS["pipeline_machine_inventory_unload"],
                "scripts": {
                    "steps": {
                        "MachineInventoryUnloadStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/machine_inventory_unload/MachineInventoryUnloadStep/",
                            "entry_script": "run_machine_inventory_unload.py",
                            "data_prefixes": {
                                "output_machine_inventory": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/reference/machine_inventory/",
                            },
                        }
                    }
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pipeline_inference_predictions", feature_spec_version),
            "spec": {
                "required_runtime_params": PIPELINE_RUNTIME_PARAMS["pipeline_inference_predictions"],
                "scripts": {
                    "steps": {
                        "InferencePredictionsStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/inference_predictions/InferencePredictionsStep/",
                            "entry_script": "run_inference_predictions.py",
                            "data_prefixes": {
                                "input_fg_a": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_a/",
                                "input_fg_c": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_c/",
                                "output_predictions": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/inference/predictions/",
                            },
                        }
                    }
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pipeline_prediction_feature_join", feature_spec_version),
            "spec": {
                "required_runtime_params": PIPELINE_RUNTIME_PARAMS["pipeline_prediction_feature_join"],
                "scripts": {
                    "steps": {
                        "PredictionFeatureJoinStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/prediction_feature_join/PredictionFeatureJoinStep/",
                            "entry_script": "run_prediction_feature_join.py",
                            "data_prefixes": {
                                "input_predictions": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/inference/predictions/",
                                "output_join": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/publication/prediction_feature_join/",
                            },
                        }
                    }
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pipeline_if_training", feature_spec_version),
            "spec": {
                "required_runtime_params": PIPELINE_RUNTIME_PARAMS["pipeline_if_training"],
                "scripts": {
                    "steps": {
                        "IFTrainingStep": {
                            "code_prefix_s3": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/code/pipelines/if_training/IFTrainingStep/",
                            "entry_script": "run_if_training.py",
                            "data_prefixes": {
                                "input_fg_a": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_a/",
                                "input_fg_c": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/features/fg_c/",
                                "output_training": "s3://<bucket>/projects/<project_name>/versions/<feature_spec_version>/data/training/if_training/",
                            },
                        }
                    }
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
    ]

    job_items = [
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("delta_builder", feature_spec_version),
            "spec": {
                "project_name": project_name,
                "job_name": "delta_builder",
                "feature_spec_version": feature_spec_version,
                "input": {
                    "s3_prefix": "s3://REPLACE_ME/input/",
                    "format": "json",
                    "compression": "gzip",
                },
                "dq": {
                    "drop_malformed_ip": True,
                    "duration_non_negative": True,
                    "bytes_non_negative": True,
                    "filter_null_bytes_ports": True,
                    "emit_metrics": True,
                },
                "enrichment": {
                    "vdi_hostname_prefixes": [],
                    "port_sets_location": "s3://REPLACE_ME/config/port_sets.json",
                },
                "roles": [
                    {
                        "name": "src_to_dst",
                        "host_ip": "src_ip",
                        "peer_ip": "dst_ip",
                        "bytes_sent": "bytes_out",
                        "bytes_recv": "bytes_in",
                        "peer_port": "dst_port",
                    }
                ],
                "operators": [{"type": "window_15m", "params": {}}],
                "output": {
                    "s3_prefix": "s3://REPLACE_ME/delta/",
                    "format": "parquet",
                    "partition_keys": ["dt", "hour"],
                    "write_mode": "append",
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("fg_a_builder", feature_spec_version),
            "spec": {
                "delta_input": {"s3_prefix": "s3://REPLACE_ME/delta/"},
                "fg_a_output": {"s3_prefix": "s3://REPLACE_ME/fg_a/"},
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("pair_counts_builder", feature_spec_version),
            "spec": {
                "traffic_input": {"s3_prefix": "s3://REPLACE_ME/traffic/"},
                "pair_counts_output": {"s3_prefix": "s3://REPLACE_ME/pair_counts/"},
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("fg_b_builder", feature_spec_version),
            "spec": {
                "horizons": ["7d", "30d"],
                "fg_a_input": {"s3_prefix": "s3://REPLACE_ME/fg_a/"},
                "fg_b_output": {"s3_prefix": "s3://REPLACE_ME/fg_b/"},
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("fg_c_builder", feature_spec_version),
            "spec": {
                "fg_a_input": {"s3_prefix": "s3://REPLACE_ME/fg_a/"},
                "fg_b_input": {"s3_prefix": "s3://REPLACE_ME/fg_b/"},
                "fg_c_output": {"s3_prefix": "s3://REPLACE_ME/fg_c/"},
                "horizons": ["7d", "30d"],
                "join_keys": ["host_ip", "window_label"],
                "metrics": ["sessions_cnt_w_15m", "bytes_src_sum_w_15m"],
                "eps": 1e-6,
                "z_max": 6.0,
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("machine_inventory_unload", feature_spec_version),
            "spec": {
                "processing_image_uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/ndr-pyspark:latest",
                "redshift": {
                    "cluster_identifier": "REPLACE_ME",
                    "database": "REPLACE_ME",
                    "secret_arn": "REPLACE_ME",
                    "region": "REPLACE_ME",
                    "iam_role": "REPLACE_ME",
                },
                "query": {"schema": "public", "table": "REPLACE_ME"},
                "output": {
                    "s3_prefix": "s3://REPLACE_ME/machine_inventory/",
                    "output_format": "PARQUET",
                    "partitioning": ["snapshot_month"],
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("prediction_feature_join", feature_spec_version),
            "spec": {
                "destination": {
                    "type": "s3",
                    "s3": {
                        "s3_prefix": "s3://REPLACE_ME/prediction_feature_join/",
                        "format": "PARQUET",
                    },
                }
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("inference_predictions", feature_spec_version),
            "spec": {
                "feature_inputs": {
                    "fg_a": {"s3_prefix": "s3://REPLACE_ME/fg_a/", "required": True},
                    "fg_c": {"s3_prefix": "s3://REPLACE_ME/fg_c/", "required": True},
                },
                "model": {"endpoint_name": "REPLACE_ME"},
                "output": {"s3_prefix": "s3://REPLACE_ME/predictions/", "format": "PARQUET"},
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("if_training", feature_spec_version),
            "spec": {
                "feature_inputs": {
                    "fg_a": {"s3_prefix": "s3://REPLACE_ME/fg_a/"},
                    "fg_c": {"s3_prefix": "s3://REPLACE_ME/fg_c/"},
                },
                "window": {"lookback_months": 4, "gap_months": 1},
                "model": {"version": feature_spec_version},
                "output": {
                    "artifacts_s3_prefix": "s3://REPLACE_ME/if_training/",
                    "report_s3_prefix": "s3://REPLACE_ME/if_training/reports/",
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
        {
            "project_name": project_name,
            "job_name": _versioned_job_name("project_parameters", feature_spec_version),
            "spec": {
                "ip_machine_mapping_s3_prefix": "s3://REPLACE_ME/config/ip_machine_mapping/",
                "defaults": {
                    "ProjectName": "<project_name>",
                    "FeatureSpecVersion": "<feature_spec_version>",
                    "MiniBatchId": "auto-mini-batch",
                    "BatchStartTsIso": "<required:BatchStartTsIso>",
                    "BatchEndTsIso": "<required:BatchEndTsIso>",
                    "ReferenceTimeIso": "<required:ReferenceTimeIso>",
                    "ReferenceMonthIso": "<required:ReferenceMonthIso>",
                    "RunId": "manual-run",
                    "ExecutionTsIso": "<required:ExecutionTsIso>",
                },
                "validation": {
                    "FeatureSpecVersion": "^[a-zA-Z0-9_.-]+$",
                    "ProjectName": "^[a-zA-Z0-9_.-]+$",
                },
            },
            "feature_spec_version": feature_spec_version,
            "updated_at": now,
            "owner": owner,
        },
    ]

    items = [*pipeline_items, *job_items]
    _validate_seed_item_contracts(items)
    return items


def _validate_seed_item_contracts(items: list[dict[str, Any]]) -> None:
    """Fail fast when seeded pipeline script/runtime contracts are incomplete."""
    by_job_name = {item["job_name"].split(JOB_SPEC_SORT_KEY_DELIMITER, 1)[0]: item for item in items}

    for pipeline_job_name, required_params in PIPELINE_RUNTIME_PARAMS.items():
        item = by_job_name.get(pipeline_job_name)
        if not item:
            raise ValueError(f"Missing required pipeline seed item: {pipeline_job_name}")

        spec = item.get("spec", {})
        present_params = spec.get("required_runtime_params")
        if present_params != required_params:
            raise ValueError(
                f"Pipeline '{pipeline_job_name}' required_runtime_params mismatch: "
                f"expected {required_params}, got {present_params}"
            )

        scripts = spec.get("scripts", {})
        steps = scripts.get("steps")
        if not isinstance(steps, dict) or not steps:
            raise ValueError(f"Pipeline '{pipeline_job_name}' missing scripts.steps mapping")

        for step_name, step_spec in steps.items():
            if not isinstance(step_spec, dict):
                raise ValueError(f"Pipeline '{pipeline_job_name}' step '{step_name}' spec must be an object")
            if not step_spec.get("code_prefix_s3"):
                raise ValueError(f"Pipeline '{pipeline_job_name}' step '{step_name}' missing code_prefix_s3")
            if not step_spec.get("entry_script"):
                raise ValueError(f"Pipeline '{pipeline_job_name}' step '{step_name}' missing entry_script")
            data_prefixes = step_spec.get("data_prefixes")
            if not isinstance(data_prefixes, dict) or not data_prefixes:
                raise ValueError(f"Pipeline '{pipeline_job_name}' step '{step_name}' missing data_prefixes")


def _load_custom_seed_items(custom_seed_json: str) -> list[dict[str, Any]]:
    """Load custom seed items from JSON text or a JSON file path.

    Parameters
    ----------
    custom_seed_json:
        Either raw JSON content (list of items) or a path to a ``.json`` file
        containing that list.

    Returns
    -------
    list[dict[str, Any]]
        Parsed custom seed items.
    """
    source_text = custom_seed_json.strip()
    if not source_text:
        raise ValueError("custom_seed_json must be non-empty for seed_profile=custom-json")

    candidate_path = Path(source_text)
    if candidate_path.exists() and candidate_path.is_file():
        source_text = candidate_path.read_text(encoding="utf-8")

    payload = json.loads(source_text)
    if not isinstance(payload, list):
        raise ValueError("Custom seed JSON must be a list of item objects")

    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"Custom seed item at index {idx} must be a dictionary")

    return payload


def _resolve_seed_items(
    seed_profile: str,
    project_name: str,
    feature_spec_version: str,
    owner: str,
    custom_seed_json: str | None,
) -> list[dict[str, Any]]:
    """Resolve seed items from the selected profile.

    Supported profiles:
    - ``ndr``: uses hard-coded NDR seed templates.
    - ``none``: writes no seed items.
    - ``custom-json``: reads seed items from JSON string/file.
    """
    if seed_profile == NDR_SEED_PROFILE:
        return _build_bootstrap_items(project_name, feature_spec_version, owner)
    if seed_profile == NO_SEED_PROFILE:
        return []
    if seed_profile == CUSTOM_JSON_SEED_PROFILE:
        if custom_seed_json is None:
            raise ValueError(
                "custom_seed_json is required when seed_profile='custom-json'"
            )
        return _load_custom_seed_items(custom_seed_json)

    raise ValueError(
        f"Unsupported seed_profile='{seed_profile}'. Expected one of {VALID_SEED_PROFILES}."
    )


def upsert_items(table_name: str, items: list[dict[str, Any]], region_name: str | None = None) -> int:
    """Validate and upsert bootstrap items into DynamoDB.

    Parameters
    ----------
    table_name:
        Destination DynamoDB table.
    items:
        Seed items to write.
    region_name:
        Optional AWS region override.

    Returns
    -------
    int
        Number of written items.
    """
    table = boto3.resource("dynamodb", region_name=region_name).Table(table_name)
    required_item_keys = {"project_name", "job_name", "spec"}

    with table.batch_writer(overwrite_by_pkeys=["project_name", "job_name"]) as writer:
        for item in items:
            missing = required_item_keys - set(item.keys())
            if missing:
                raise ValueError(f"Item missing required keys {sorted(missing)}: {item}")
            writer.put_item(Item=item)

    return len(items)


def provision_table(
    project_name: str,
    feature_spec_version: str,
    owner: str = "ndr-team",
    table_name: str | None = None,
    region_name: str | None = None,
    use_json_table_definition: bool = False,
    seed_profile: str = NDR_SEED_PROFILE,
    custom_seed_json: str | None = None,
    create_routing_table: bool = False,
    routing_table_name: str | None = None,
    org1: str | None = None,
    org2: str | None = None,
    ingestion_prefix: str | None = None,
) -> dict[str, Any]:
    """Create table and upsert starter records.

    Parameters
    ----------
    project_name:
        Partition-key value for seeded records.
    feature_spec_version:
        Job-spec version component for sort keys.
    owner:
        Metadata owner for seeded records.
    table_name:
        Optional table-name override.
    region_name:
        Optional AWS region override.
    use_json_table_definition:
        If ``True``, ``TABLE_SPEC_JSON`` is used to construct the table creation
        request payload.
    seed_profile:
        Item seeding strategy. Supported values: ``ndr``, ``none``,
        ``custom-json``.
    custom_seed_json:
        JSON list (or path to JSON file) for custom item seeding when
        ``seed_profile='custom-json'``.
    create_routing_table:
        When ``True``, also create/validate the org-path routing table.
    routing_table_name:
        Optional routing table name override.
    org1, org2, ingestion_prefix:
        Optional seed values for a routing record.

    Returns
    -------
    dict[str, Any]
        Summary containing table and write information.
    """
    resolved_table_name = resolve_table_name(table_name)
    table_description = create_table_if_missing(
        resolved_table_name,
        region_name=region_name,
        use_json_table_definition=use_json_table_definition,
    )
    items = _resolve_seed_items(
        seed_profile=seed_profile,
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        owner=owner,
        custom_seed_json=custom_seed_json,
    )
    written = 0
    if items:
        written = upsert_items(resolved_table_name, items, region_name=region_name)

    routing_table_status = None
    routing_items_written = 0
    resolved_routing_table_name = None
    if create_routing_table:
        resolved_routing_table_name = resolve_routing_table_name(routing_table_name)
        routing_description = create_routing_table_if_missing(
            resolved_routing_table_name,
            region_name=region_name,
        )
        routing_table_status = routing_description.get("TableStatus")
        if org1 and org2 and ingestion_prefix:
            routing_items_written = upsert_routing_items(
                resolved_routing_table_name,
                [
                    {
                        "org_key": f"{org1}#{org2}",
                        "project_name": project_name,
                        "ingestion_prefix": ingestion_prefix,
                        "feature_spec_version": feature_spec_version,
                        "updated_at": _utc_now_iso8601(),
                    }
                ],
                region_name=region_name,
            )

    return {
        "table_name": resolved_table_name,
        "table_status": table_description.get("TableStatus"),
        "items_written": written,
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "used_json_table_definition": use_json_table_definition,
        "seed_profile": seed_profile,
        "routing_table_name": resolved_routing_table_name,
        "routing_table_status": routing_table_status,
        "routing_items_written": routing_items_written,
    }


def run_from_notebook(params: dict[str, Any]) -> dict[str, Any]:
    """Provision the table directly from notebook code using a parameter dict.

    Expected parameter keys
    -----------------------
    Required:
    - ``project_name``
    - ``feature_spec_version``

    Optional:
    - ``owner`` (default: ``ndr-team``)
    - ``table_name``
    - ``region_name``
    - ``use_json_table_definition`` (default: ``False``)
    - ``seed_profile`` (default: ``ndr``)
    - ``custom_seed_json`` (required for ``seed_profile='custom-json'``)
    - ``create_routing_table`` (default: ``False``)
    - ``routing_table_name``
    - ``org1``, ``org2``, ``ingestion_prefix`` (optional routing seed)

    Parameters
    ----------
    params:
        Dictionary of parameters provided by a notebook cell.

    Returns
    -------
    dict[str, Any]
        Provisioning summary from :func:`provision_table`.
    """
    required = ["project_name", "feature_spec_version"]
    missing = [key for key in required if not params.get(key)]
    if missing:
        raise ValueError(f"Missing required notebook params: {missing}")

    return provision_table(
        project_name=params["project_name"],
        feature_spec_version=params["feature_spec_version"],
        owner=params.get("owner", "ndr-team"),
        table_name=params.get("table_name"),
        region_name=params.get("region_name"),
        use_json_table_definition=bool(params.get("use_json_table_definition", False)),
        seed_profile=params.get("seed_profile", NDR_SEED_PROFILE),
        custom_seed_json=params.get("custom_seed_json"),
        create_routing_table=bool(params.get("create_routing_table", False)),
        routing_table_name=params.get("routing_table_name"),
        org1=params.get("org1"),
        org2=params.get("org2"),
        ingestion_prefix=params.get("ingestion_prefix"),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for table provisioning."""
    parser = argparse.ArgumentParser(
        description="Create and seed the ML projects parameters DynamoDB table.",
    )
    parser.add_argument("--project-name", required=False, help="Project partition key value.")
    parser.add_argument(
        "--feature-spec-version",
        required=False,
        help="Feature-spec version used in versioned job_name keys (e.g. v1).",
    )
    parser.add_argument("--owner", default="ndr-team", help="Item owner/auditing attribute.")
    parser.add_argument(
        "--table-name",
        default=None,
        help=(
            "Override table name. Defaults to ML_PROJECTS_PARAMETERS_TABLE_NAME, "
            "then JOB_SPEC_DDB_TABLE_NAME, then ml_projects_parameters."
        ),
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Optional AWS region override for boto3 clients/resources.",
    )
    parser.add_argument(
        "--use-json-table-definition",
        action="store_true",
        help="Use hard-coded TABLE_SPEC_JSON.create_table_request payload for table creation.",
    )
    parser.add_argument(
        "--print-table-spec-json",
        action="store_true",
        help="Print the hard-coded TABLE_SPEC_JSON object and exit.",
    )
    parser.add_argument(
        "--seed-profile",
        default=NDR_SEED_PROFILE,
        choices=VALID_SEED_PROFILES,
        help=(
            "Seeding profile: 'ndr' for hard-coded NDR starter items, 'none' to skip seeding, "
            "or 'custom-json' to use --custom-seed-json."
        ),
    )
    parser.add_argument(
        "--custom-seed-json",
        default=None,
        help=(
            "Custom seed items JSON list or path to a JSON file. Required when "
            "--seed-profile custom-json."
        ),
    )
    parser.add_argument(
        "--create-routing-table",
        action="store_true",
        help="Also create the org-path routing table (ml_projects_routing by default).",
    )
    parser.add_argument(
        "--routing-table-name",
        default=None,
        help="Override routing table name. Defaults to ML_PROJECTS_ROUTING_TABLE_NAME or ml_projects_routing.",
    )
    parser.add_argument("--org1", default=None, help="Org convention 1 value for optional routing seed.")
    parser.add_argument("--org2", default=None, help="Org convention 2 value for optional routing seed.")
    parser.add_argument(
        "--ingestion-prefix",
        default=None,
        help="Ingestion S3 prefix for optional routing seed (requires --org1 and --org2).",
    )
    args = parser.parse_args(argv)

    if not args.print_table_spec_json:
        missing = []
        if not args.project_name:
            missing.append("--project-name")
        if not args.feature_spec_version:
            missing.append("--feature-spec-version")
        if missing:
            parser.error(f"Missing required arguments: {', '.join(missing)}")

    return args


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for module execution."""
    args = parse_args(argv)

    if args.print_table_spec_json:
        print(json.dumps(TABLE_SPEC_JSON, indent=2, sort_keys=True))
        return 0

    summary = provision_table(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        owner=args.owner,
        table_name=args.table_name,
        region_name=args.region,
        use_json_table_definition=args.use_json_table_definition,
        seed_profile=args.seed_profile,
        custom_seed_json=args.custom_seed_json,
        create_routing_table=args.create_routing_table,
        routing_table_name=args.routing_table_name,
        org1=args.org1,
        org2=args.org2,
        ingestion_prefix=args.ingestion_prefix,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
