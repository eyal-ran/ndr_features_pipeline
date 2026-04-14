from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ndr.orchestration.code_artifact_contracts import (
    CODE_ARTIFACT_VALIDATE_REPORT_VERSION,
    validate_code_artifact_validate_report,
    validate_code_bundle_build_output,
)

DEFAULT_REPORT_OUT = "/tmp/code_artifact_validate_report.json"
ERROR_CODE_OBJECT_MISSING = "CODE_ARTIFACT_OBJECT_MISSING"
ERROR_CODE_DOWNLOAD_FAILED = "CODE_ARTIFACT_DOWNLOAD_FAILED"
ERROR_CODE_HASH_MISMATCH = "CODE_ARTIFACT_HASH_MISMATCH"
ERROR_CODE_ARCHIVE_INVALID = "CODE_ARTIFACT_ARCHIVE_INVALID"
ERROR_CODE_ENTRY_SCRIPT_MISSING = "CODE_ARTIFACT_ENTRY_SCRIPT_MISSING"
ERROR_CODE_SCHEMA_INVALID = "CODE_ARTIFACT_SCHEMA_INVALID"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate code artifact manifest/hash contracts")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--artifact-build-id", required=True)
    parser.add_argument("--build-manifest-in", required=True)
    parser.add_argument("--validation-report-out", default=DEFAULT_REPORT_OUT)
    parser.add_argument("--region-name")
    return parser.parse_args()


def _load_boto3() -> Any:
    return importlib.import_module("boto3")


def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid s3 uri: {uri!r}")
    no_scheme = uri[5:]
    bucket, _, key = no_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid s3 uri: {uri!r}")
    return bucket, key


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _error_code_from_exception(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or exc.__class__.__name__)
    return exc.__class__.__name__


def _is_missing_object_error(exc: Exception) -> bool:
    return _error_code_from_exception(exc) in {"404", "NoSuchKey", "NotFound"}


def _download_to_temp(*, s3_client: Any, uri: str) -> Path:
    bucket, key = _split_s3_uri(uri)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    local_dir = Path(tempfile.mkdtemp(prefix="ndr-artifact-validate-"))
    local_path = local_dir / "artifact.bin"
    local_path.write_bytes(body)
    return local_path


def _assert_archive_and_entry(*, archive_path: Path, expected_format: str, entry_script: str) -> None:
    if expected_format != "tar.gz":
        raise ValueError(f"Unsupported artifact_format for validator: {expected_format!r}")
    try:
        with tarfile.open(archive_path, mode="r:gz") as tf:
            names = {m.name for m in tf.getmembers()}
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(f"Invalid tar.gz archive: {exc}") from exc

    if entry_script not in names:
        raise FileNotFoundError(f"entry_script '{entry_script}' not present in artifact")


@dataclass(frozen=True)
class StepResult:
    artifact_family: str
    pipeline_job_name: str
    step_name: str
    status: str
    error_code: str
    error_message: str
    retriable: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "artifact_family": self.artifact_family,
            "pipeline_job_name": self.pipeline_job_name,
            "step_name": self.step_name,
            "status": self.status,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "retriable": self.retriable,
        }


def _validate_manifest_against_args(*, args: argparse.Namespace, payload: dict[str, Any]) -> None:
    expected_pairs = {
        "project_name": args.project_name,
        "feature_spec_version": args.feature_spec_version,
        "artifact_build_id": args.artifact_build_id,
    }
    for field_name, expected in expected_pairs.items():
        observed = str(payload.get(field_name) or "")
        if observed != expected:
            raise ValueError(f"{field_name} mismatch: expected {expected!r}, got {observed!r}")


def _step_sort_key(step: dict[str, Any]) -> tuple[str, str]:
    return (str(step.get("pipeline_job_name") or ""), str(step.get("step_name") or ""))


def validate_artifacts(args: argparse.Namespace) -> dict[str, Any]:
    build_payload = json.loads(Path(args.build_manifest_in).read_text(encoding="utf-8"))
    validate_code_bundle_build_output(build_payload)
    _validate_manifest_against_args(args=args, payload=build_payload)

    boto3 = _load_boto3()
    client_kwargs: dict[str, Any] = {"service_name": "s3"}
    if args.region_name:
        client_kwargs["region_name"] = args.region_name
    s3_client = boto3.client(**client_kwargs)

    step_results: list[StepResult] = []
    ordered_steps = sorted(build_payload["step_artifacts"], key=_step_sort_key)

    for step in ordered_steps:
        uri = step["code_artifact_s3_uri"]
        bucket, key = _split_s3_uri(uri)
        try:
            head = s3_client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            if _is_missing_object_error(exc):
                step_results.append(
                    StepResult(
                        artifact_family=step["artifact_family"],
                        pipeline_job_name=step["pipeline_job_name"],
                        step_name=step["step_name"],
                        status="FAIL",
                        error_code=ERROR_CODE_OBJECT_MISSING,
                        error_message=f"Artifact object missing at {uri}",
                        retriable=False,
                    )
                )
            else:
                code = _error_code_from_exception(exc)
                step_results.append(
                    StepResult(
                        artifact_family=step["artifact_family"],
                        pipeline_job_name=step["pipeline_job_name"],
                        step_name=step["step_name"],
                        status="FAIL",
                        error_code=ERROR_CODE_DOWNLOAD_FAILED,
                        error_message=f"S3 HEAD failed ({code}) for {uri}",
                        retriable=True,
                    )
                )
            break

        size = int(head.get("ContentLength") or 0)
        if size <= 0:
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_OBJECT_MISSING,
                    error_message=f"Artifact object at {uri} is empty",
                    retriable=False,
                )
            )
            break

        try:
            local_path = _download_to_temp(s3_client=s3_client, uri=uri)
        except Exception as exc:
            code = _error_code_from_exception(exc)
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_DOWNLOAD_FAILED,
                    error_message=f"S3 download failed ({code}) for {uri}",
                    retriable=True,
                )
            )
            break

        observed_sha = _sha256_file(local_path)
        expected_sha = str(step["artifact_sha256"])
        if observed_sha != expected_sha:
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_HASH_MISMATCH,
                    error_message=(
                        f"Artifact hash mismatch for {uri}: expected={expected_sha} observed={observed_sha}"
                    ),
                    retriable=False,
                )
            )
            break

        try:
            _assert_archive_and_entry(
                archive_path=local_path,
                expected_format=str(step["artifact_format"]),
                entry_script=str(step["entry_script"]),
            )
        except FileNotFoundError as exc:
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_ENTRY_SCRIPT_MISSING,
                    error_message=str(exc),
                    retriable=False,
                )
            )
            break
        except Exception as exc:
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_ARCHIVE_INVALID,
                    error_message=str(exc),
                    retriable=False,
                )
            )
            break

        step_results.append(
            StepResult(
                artifact_family=step["artifact_family"],
                pipeline_job_name=step["pipeline_job_name"],
                step_name=step["step_name"],
                status="PASS",
                error_code="OK",
                error_message="",
                retriable=False,
            )
        )

    report = {
        "contract_version": CODE_ARTIFACT_VALIDATE_REPORT_VERSION,
        "artifact_build_id": args.artifact_build_id,
        "status": "FAIL" if any(item.status == "FAIL" for item in step_results) else "PASS",
        "validated_steps": sum(1 for item in step_results if item.status == "PASS"),
        "step_results": [result.as_dict() for result in step_results],
    }
    validate_code_artifact_validate_report(report)

    out_path = Path(args.validation_report_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, sort_keys=True, indent=2), encoding="utf-8")
    return report


def main() -> None:
    args = parse_args()
    try:
        report = validate_artifacts(args)
    except Exception as exc:
        payload = {
            "status": "FAIL",
            "error_code": ERROR_CODE_SCHEMA_INVALID,
            "message": str(exc),
        }
        print(json.dumps(payload, sort_keys=True), file=sys.stderr)
        raise SystemExit(2) from exc
    print(json.dumps(report, sort_keys=True))
    if report["status"] == "FAIL":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
