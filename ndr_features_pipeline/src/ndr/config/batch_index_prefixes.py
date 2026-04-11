"""Canonical Batch Index S3-prefix precomputation for RT/backfill writers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ndr.contracts import DPP_CODE_STEP_KEYS, MLP_CODE_STEP_KEYS


@dataclass(frozen=True)
class BatchPrefixPrecomputeRequest:
    project_name: str
    batch_id: str
    raw_parsed_logs_s3_prefix: str
    etl_ts: str
    ml_project_names: list[str]
    dpp_roots: dict[str, str]
    mlp_roots_by_project: dict[str, dict[str, str]]
    dpp_code_paths: dict[str, str]
    mlp_code_paths_by_project: dict[str, dict[str, str]]
    dpp_code_metadata: dict[str, dict[str, str]]
    mlp_code_metadata_by_project: dict[str, dict[str, dict[str, str]]]


@dataclass(frozen=True)
class BatchPrefixPrecomputeResult:
    date_partition: str
    hour: str
    within_hour_run_number: str
    org1: str
    org2: str
    s3_prefixes: dict[str, Any]


def precompute_batch_index_prefixes(request: BatchPrefixPrecomputeRequest) -> BatchPrefixPrecomputeResult:
    parsed = _derive_batch_dimensions(
        raw_parsed_logs_s3_prefix=request.raw_parsed_logs_s3_prefix,
        etl_ts=request.etl_ts,
    )
    dpp = _build_dpp_prefixes(
        project_name=request.project_name,
        org1=parsed["org1"],
        org2=parsed["org2"],
        date_partition=parsed["date_partition"],
        hour=parsed["hour"],
        within_hour_run_number=parsed["within_hour_run_number"],
        roots=request.dpp_roots,
        code_paths=request.dpp_code_paths,
        code_metadata=request.dpp_code_metadata,
    )
    mlp = _build_mlp_prefixes(
        ml_project_names=request.ml_project_names,
        org1=parsed["org1"],
        org2=parsed["org2"],
        date_partition=parsed["date_partition"],
        hour=parsed["hour"],
        within_hour_run_number=parsed["within_hour_run_number"],
        roots_by_project=request.mlp_roots_by_project,
        code_paths_by_project=request.mlp_code_paths_by_project,
        code_metadata_by_project=request.mlp_code_metadata_by_project,
    )
    return BatchPrefixPrecomputeResult(
        date_partition=parsed["date_partition"],
        hour=parsed["hour"],
        within_hour_run_number=parsed["within_hour_run_number"],
        org1=parsed["org1"],
        org2=parsed["org2"],
        s3_prefixes={"dpp": dpp, "mlp": mlp},
    )


def _build_dpp_prefixes(
    *,
    project_name: str,
    org1: str,
    org2: str,
    date_partition: str,
    hour: str,
    within_hour_run_number: str,
    roots: dict[str, str],
    code_paths: dict[str, str],
    code_metadata: dict[str, dict[str, str]],
) -> dict[str, Any]:
    for required in ("delta_root", "pair_counts_root", "fg_a_root", "fg_b_root", "fg_c_root", "machine_inventory_root"):
        if required not in roots:
            raise KeyError(f"Missing DPP root: {required}")

    base = f"{project_name}/{org1}/{org2}/{date_partition}/{hour}/{within_hour_run_number}"
    dpp: dict[str, Any] = {
        "delta": f"{_clean_root(roots['delta_root'])}/{base}/delta/part-00000.parquet",
        "pair_counts": f"{_clean_root(roots['pair_counts_root'])}/{base}/pair_counts/part-00000.parquet",
        "fg_a": f"{_clean_root(roots['fg_a_root'])}/{base}/fg_a/features/part-00000.parquet",
        "fg_a_subpaths": {
            "features": f"{_clean_root(roots['fg_a_root'])}/{base}/fg_a/features/part-00000.parquet",
            "metadata": f"{_clean_root(roots['fg_a_root'])}/{base}/fg_a/metadata/schema.json",
        },
        "fg_c": f"{_clean_root(roots['fg_c_root'])}/{base}/fg_c/part-00000.parquet",
        "machine_inventory": f"{_clean_root(roots['machine_inventory_root'])}/{base}/machine_inventory/snapshot.parquet",
        "fg_b": {
            "machines_manifest": f"{_clean_root(roots['fg_b_root'])}/{base}/fg_b/machines_manifest/manifest.json",
            "machines_unload_for_update": f"{_clean_root(roots['fg_b_root'])}/{base}/fg_b/machines_unload_for_update/unload_000.parquet",
            "machines_base_stats": f"{_clean_root(roots['fg_b_root'])}/{base}/fg_b/machines_base_stats/part-00000.parquet",
            "segment_base_stats": f"{_clean_root(roots['fg_b_root'])}/{base}/fg_b/segment_base_stats/part-00000.parquet",
        },
    }
    dpp["code"] = _validate_code_paths(code_paths, DPP_CODE_STEP_KEYS, "dpp")
    dpp["code_metadata"] = _validate_code_metadata(code_metadata, DPP_CODE_STEP_KEYS, "dpp")
    return dpp


def _build_mlp_prefixes(
    *,
    ml_project_names: list[str],
    org1: str,
    org2: str,
    date_partition: str,
    hour: str,
    within_hour_run_number: str,
    roots_by_project: dict[str, dict[str, str]],
    code_paths_by_project: dict[str, dict[str, str]],
    code_metadata_by_project: dict[str, dict[str, dict[str, str]]],
) -> dict[str, Any]:
    if not ml_project_names:
        raise ValueError("ml_project_names cannot be empty")

    out: dict[str, Any] = {}
    for ml_project_name in ml_project_names:
        roots = roots_by_project.get(ml_project_name)
        if not roots:
            raise KeyError(f"Missing MLP roots for ml_project_name={ml_project_name}")
        for required in ("predictions_root", "prediction_join_root", "publication_root", "training_reports_root", "training_artifacts_root", "production_model_root"):
            if required not in roots:
                raise KeyError(f"Missing MLP root {required} for ml_project_name={ml_project_name}")

        path_suffix = f"{org1}/{org2}/{date_partition}/{hour}/{within_hour_run_number}"
        code_paths = code_paths_by_project.get(ml_project_name, {})
        code_metadata = code_metadata_by_project.get(ml_project_name, {})
        out[ml_project_name] = {
            "predictions": f"{_clean_root(roots['predictions_root'])}/{path_suffix}/predictions/part-00000.parquet",
            "prediction_join": f"{_clean_root(roots['prediction_join_root'])}/{path_suffix}/prediction_join/part-00000.parquet",
            "publication": f"{_clean_root(roots['publication_root'])}/{path_suffix}/publication/publication_payload.json",
            "training_events": {
                "training_reports": f"{_clean_root(roots['training_reports_root'])}/<run_id_from_training_script>/reports/final_training_report.json",
                "training_artifacts": f"{_clean_root(roots['training_artifacts_root'])}/<run_id_from_training_script>/artifacts/model/model.tar.gz",
            },
            "production_artifacts": {
                "inference_model": f"{_clean_root(roots['production_model_root'])}/model.tar.gz",
            },
            "code": _validate_code_paths(code_paths, MLP_CODE_STEP_KEYS, f"mlp[{ml_project_name}]"),
            "code_metadata": _validate_code_metadata(code_metadata, MLP_CODE_STEP_KEYS, f"mlp[{ml_project_name}]"),
        }
    return out


def _validate_code_paths(code_paths: dict[str, str], step_keys: tuple[str, ...], label: str) -> dict[str, str]:
    missing = [step_key for step_key in step_keys if step_key not in code_paths]
    if missing:
        raise KeyError(f"Missing code paths for {label}: {missing}")
    return {step_key: str(code_paths[step_key]) for step_key in step_keys}


def _validate_code_metadata(
    metadata: dict[str, dict[str, str]],
    step_keys: tuple[str, ...],
    label: str,
) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for step_key in step_keys:
        values = metadata.get(step_key)
        if not values:
            raise KeyError(f"Missing code metadata for {label}.{step_key}")
        for required in ("code_artifact_s3_uri", "artifact_build_id", "artifact_sha256", "artifact_format"):
            if required not in values:
                raise KeyError(f"Missing {required} in {label}.{step_key}")
        out[step_key] = {
            "code_artifact_s3_uri": str(values["code_artifact_s3_uri"]),
            "artifact_build_id": str(values["artifact_build_id"]),
            "artifact_sha256": str(values["artifact_sha256"]),
            "artifact_format": str(values["artifact_format"]),
        }
        if "artifact_generated_at_utc" in values:
            out[step_key]["artifact_generated_at_utc"] = str(values["artifact_generated_at_utc"])
    return out


def _derive_batch_dimensions(*, raw_parsed_logs_s3_prefix: str, etl_ts: str) -> dict[str, str]:
    parts = raw_parsed_logs_s3_prefix.strip("/").split("/")
    if len(parts) < 8:
        raise ValueError("raw_parsed_logs_s3_prefix does not match canonical ingestion path")
    org1 = parts[4]
    org2 = parts[5]

    ts = datetime.fromisoformat(etl_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return {
        "org1": org1,
        "org2": org2,
        "date_partition": ts.strftime("%Y/%m/%d"),
        "hour": ts.strftime("%H"),
        "within_hour_run_number": str((ts.minute // 15) + 1),
    }


def _clean_root(value: str) -> str:
    return value.rstrip("/")
