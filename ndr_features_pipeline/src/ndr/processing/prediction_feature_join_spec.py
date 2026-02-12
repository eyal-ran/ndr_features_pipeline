"""NDR prediction feature join spec module."""

from __future__ import annotations


from dataclasses import dataclass, field
from typing import Any, Dict, List

from ndr.processing.inference_predictions_spec import InferenceOutputSpec


@dataclass
class PredictionFeatureJoinDestinationRedshift:
    """Data container for PredictionFeatureJoinDestinationRedshift."""
    cluster_identifier: str
    database: str
    secret_arn: str
    region: str
    iam_role: str
    schema: str
    table: str
    db_user: str | None = None
    pre_sql: List[str] = field(default_factory=list)
    post_sql: List[str] = field(default_factory=list)
    copy_options: str | None = None


@dataclass
class PredictionFeatureJoinSpec:
    """Data container for PredictionFeatureJoinSpec."""
    destination_type: str
    s3_output: InferenceOutputSpec | None
    redshift_output: PredictionFeatureJoinDestinationRedshift | None


def _parse_output_spec(payload: Dict[str, Any], default_dataset: str) -> InferenceOutputSpec:
    """Execute the parse output spec stage of the workflow."""
    if "s3_prefix" not in payload:
        raise ValueError("Prediction feature join output spec requires s3_prefix")
    return InferenceOutputSpec(
        s3_prefix=payload["s3_prefix"],
        format=payload.get("format", "parquet"),
        partition_keys=payload.get("partition_keys", ["feature_spec_version", "dt"]),
        write_mode=payload.get("write_mode", "overwrite"),
        dataset=payload.get("dataset", default_dataset),
    )


def _normalize_sql_list(value: Any, name: str) -> List[str]:
    """Execute the normalize sql list stage of the workflow."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError(f"{name} must be a string or list of strings")


def parse_prediction_feature_join_spec(job_spec: Dict[str, Any]) -> PredictionFeatureJoinSpec:
    """Execute the parse prediction feature join spec stage of the workflow."""
    destination_payload = job_spec.get("destination")
    if not destination_payload:
        raise ValueError("prediction_feature_join JobSpec requires destination")

    destination_type = destination_payload.get("type")
    if not destination_type:
        raise ValueError("prediction_feature_join destination.type is required")

    destination_type = destination_type.lower()
    if destination_type == "s3":
        s3_payload = destination_payload.get("s3") or job_spec.get("join_output")
        if not s3_payload:
            raise ValueError("prediction_feature_join destination.s3 is required for type s3")
        return PredictionFeatureJoinSpec(
            destination_type="s3",
            s3_output=_parse_output_spec(s3_payload, "prediction_feature_join"),
            redshift_output=None,
        )

    if destination_type == "redshift":
        redshift_payload = destination_payload.get("redshift") or {}
        required = [
            "cluster_identifier",
            "database",
            "secret_arn",
            "region",
            "iam_role",
            "schema",
            "table",
        ]
        missing = [key for key in required if not redshift_payload.get(key)]
        if missing:
            raise ValueError(f"prediction_feature_join redshift config missing keys: {missing}")

        s3_payload = destination_payload.get("s3") or job_spec.get("join_output")
        if not s3_payload:
            raise ValueError(
                "prediction_feature_join destination.s3 or join_output is required to stage data for redshift"
            )

        return PredictionFeatureJoinSpec(
            destination_type="redshift",
            s3_output=_parse_output_spec(s3_payload, "prediction_feature_join"),
            redshift_output=PredictionFeatureJoinDestinationRedshift(
                cluster_identifier=redshift_payload["cluster_identifier"],
                database=redshift_payload["database"],
                secret_arn=redshift_payload["secret_arn"],
                region=redshift_payload["region"],
                iam_role=redshift_payload["iam_role"],
                schema=redshift_payload["schema"],
                table=redshift_payload["table"],
                db_user=redshift_payload.get("db_user"),
                pre_sql=_normalize_sql_list(redshift_payload.get("pre_sql"), "pre_sql"),
                post_sql=_normalize_sql_list(redshift_payload.get("post_sql"), "post_sql"),
                copy_options=redshift_payload.get("copy_options"),
            ),
        )

    raise ValueError(f"Unsupported prediction_feature_join destination.type={destination_type}")
