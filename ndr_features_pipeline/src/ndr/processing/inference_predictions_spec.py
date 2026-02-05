from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class InferenceFeatureInputSpec:
    s3_prefix: str
    dataset: str | None = None
    required: bool = True


@dataclass
class InferenceModelSpec:
    endpoint_name: str
    timeout_seconds: int = 60
    max_payload_mb: float = 5.0
    max_records_per_payload: int = 200
    max_retries: int = 3
    backoff_seconds: float = 1.0
    content_type: str = "application/json"
    accept: str = "application/json"
    model_version: str | None = None
    model_name: str | None = None
    max_workers: int = 1
    region: str | None = None


@dataclass
class InferencePayloadSpec:
    feature_columns: List[str] | None = None


@dataclass
class PredictionSchemaSpec:
    score_column: str = "prediction_score"
    label_column: str | None = "prediction_label"


@dataclass
class InferenceOutputSpec:
    s3_prefix: str
    format: str = "parquet"
    partition_keys: List[str] = field(default_factory=lambda: ["feature_spec_version", "dt"])
    write_mode: str = "overwrite"
    dataset: str = "inference_predictions"


@dataclass
class InferenceSpec:
    feature_inputs: Dict[str, InferenceFeatureInputSpec]
    join_keys: List[str]
    output: InferenceOutputSpec
    model: InferenceModelSpec
    payload: InferencePayloadSpec
    prediction_schema: PredictionSchemaSpec
    join_output: InferenceOutputSpec | None = None


@dataclass
class InferencePredictionsRuntimeConfig:
    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str


def _parse_output_spec(payload: Dict[str, Any], default_dataset: str) -> InferenceOutputSpec:
    if "s3_prefix" not in payload:
        raise ValueError("Inference output spec requires s3_prefix")
    return InferenceOutputSpec(
        s3_prefix=payload["s3_prefix"],
        format=payload.get("format", "parquet"),
        partition_keys=payload.get("partition_keys", ["feature_spec_version", "dt"]),
        write_mode=payload.get("write_mode", "overwrite"),
        dataset=payload.get("dataset", default_dataset),
    )


def parse_inference_spec(job_spec: Dict[str, Any]) -> InferenceSpec:
    feature_inputs_payload = job_spec.get("feature_inputs")
    if not feature_inputs_payload:
        raise ValueError("Inference JobSpec must include feature_inputs")

    feature_inputs: Dict[str, InferenceFeatureInputSpec] = {}
    for name, payload in feature_inputs_payload.items():
        if "s3_prefix" not in payload:
            raise ValueError(f"feature_inputs.{name} requires s3_prefix")
        feature_inputs[name] = InferenceFeatureInputSpec(
            s3_prefix=payload["s3_prefix"],
            dataset=payload.get("dataset") or name,
            required=payload.get("required", True),
        )

    join_keys = job_spec.get("join_keys", ["host_ip", "window_label", "window_end_ts"])

    model_payload = job_spec.get("model", {})
    if "endpoint_name" not in model_payload:
        raise ValueError("Inference JobSpec must include model.endpoint_name")
    model_spec = InferenceModelSpec(
        endpoint_name=model_payload["endpoint_name"],
        timeout_seconds=model_payload.get("timeout_seconds", 60),
        max_payload_mb=model_payload.get("max_payload_mb", 5.0),
        max_records_per_payload=model_payload.get("max_records_per_payload", 200),
        max_retries=model_payload.get("max_retries", 3),
        backoff_seconds=model_payload.get("backoff_seconds", 1.0),
        content_type=model_payload.get("content_type", "application/json"),
        accept=model_payload.get("accept", "application/json"),
        model_version=model_payload.get("model_version"),
        model_name=model_payload.get("model_name"),
        max_workers=model_payload.get("max_workers", 1),
        region=model_payload.get("region"),
    )

    output_payload = job_spec.get("output")
    if not output_payload:
        raise ValueError("Inference JobSpec must include output")
    output_spec = _parse_output_spec(output_payload, "inference_predictions")

    payload_spec = InferencePayloadSpec(
        feature_columns=job_spec.get("payload", {}).get("feature_columns")
    )
    prediction_schema = PredictionSchemaSpec(**job_spec.get("prediction_schema", {}))

    join_output_spec = None
    join_output_payload = job_spec.get("join_output")
    if join_output_payload:
        join_output_spec = _parse_output_spec(join_output_payload, "prediction_feature_join")

    return InferenceSpec(
        feature_inputs=feature_inputs,
        join_keys=join_keys,
        output=output_spec,
        model=model_spec,
        payload=payload_spec,
        prediction_schema=prediction_schema,
        join_output=join_output_spec,
    )
