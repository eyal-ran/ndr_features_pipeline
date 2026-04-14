from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ndr.orchestration.code_artifact_contracts import (
    CODE_SMOKE_VALIDATE_REPORT_VERSION,
    validate_code_bundle_build_output,
    validate_code_smoke_validate_report,
)

DEFAULT_REPORT_OUT = "/tmp/code_smoke_validate_report.json"
ERROR_CODE_EXECUTION_FAILED = "CODE_SMOKE_EXECUTION_FAILED"
ERROR_CODE_TIMEOUT = "CODE_SMOKE_TIMEOUT"
ERROR_CODE_IMPORT_FAILED = "CODE_SMOKE_IMPORT_FAILED"
ERROR_CODE_SCHEMA_INVALID = "CODE_SMOKE_SCHEMA_INVALID"


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run code-artifact smoke validation for promoted pointers")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--artifact-build-id", required=True)
    parser.add_argument("--build-manifest-in", required=True)
    parser.add_argument("--smoke-report-out", default=DEFAULT_REPORT_OUT)
    parser.add_argument("--region-name")
    parser.add_argument("--timeout-seconds", type=int, default=120)
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


def _download_to_temp(*, s3_client: Any, uri: str) -> Path:
    bucket, key = _split_s3_uri(uri)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    local_dir = Path(tempfile.mkdtemp(prefix="ndr-artifact-smoke-download-"))
    local_path = local_dir / "artifact.tar.gz"
    local_path.write_bytes(body)
    return local_path


def _extract_tar_gz(*, archive_path: Path) -> Path:
    extract_dir = Path(tempfile.mkdtemp(prefix="ndr-artifact-smoke-extract-"))
    try:
        with tarfile.open(archive_path, mode="r:gz") as tf:
            try:
                tf.extractall(path=extract_dir, filter="data")
            except TypeError:
                tf.extractall(path=extract_dir)
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(f"Artifact extraction failed: {exc}") from exc
    return extract_dir


def _build_smoke_env(*, extract_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    extracted_src = str((extract_dir / "src").resolve())
    env["PYTHONPATH"] = f"{extracted_src}:{current_pythonpath}" if current_pythonpath else extracted_src
    return env


def _run_smoke_command(*, entry_script: str, extract_dir: Path, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, entry_script, "--help"]
    env = _build_smoke_env(extract_dir=extract_dir)
    return subprocess.run(
        command,
        cwd=extract_dir,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout_seconds,
    )


def _run_import_probe(*, extract_dir: Path, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    env = _build_smoke_env(extract_dir=extract_dir)
    return subprocess.run(
        [sys.executable, "-c", "import ndr"],
        cwd=extract_dir,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout_seconds,
    )


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


def validate_smoke(args: argparse.Namespace) -> dict[str, Any]:
    if args.timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")

    build_payload = json.loads(Path(args.build_manifest_in).read_text(encoding="utf-8"))
    validate_code_bundle_build_output(build_payload)
    _validate_manifest_against_args(args=args, payload=build_payload)

    boto3 = _load_boto3()
    client_kwargs: dict[str, Any] = {"service_name": "s3"}
    if args.region_name:
        client_kwargs["region_name"] = args.region_name
    s3_client = boto3.client(**client_kwargs)

    step_results: list[StepResult] = []
    for step in sorted(build_payload["step_artifacts"], key=_step_sort_key):
        try:
            local_artifact = _download_to_temp(s3_client=s3_client, uri=step["code_artifact_s3_uri"])
            extract_dir = _extract_tar_gz(archive_path=local_artifact)
            result = _run_smoke_command(
                entry_script=str(step["entry_script"]),
                extract_dir=extract_dir,
                timeout_seconds=args.timeout_seconds,
            )

            if result.returncode == 0:
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
                continue

            import_probe = _run_import_probe(extract_dir=extract_dir, timeout_seconds=args.timeout_seconds)
            if import_probe.returncode != 0:
                step_results.append(
                    StepResult(
                        artifact_family=step["artifact_family"],
                        pipeline_job_name=step["pipeline_job_name"],
                        step_name=step["step_name"],
                        status="FAIL",
                        error_code=ERROR_CODE_IMPORT_FAILED,
                        error_message=(
                            f"Entry script failed and import probe failed for {step['entry_script']}; "
                            f"entry_rc={result.returncode}; import_stderr={import_probe.stderr.strip()}"
                        ),
                        retriable=False,
                    )
                )
                continue

            stderr_text = (result.stderr or "").strip()
            stdout_text = (result.stdout or "").strip()
            detail = stderr_text if stderr_text else stdout_text
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_EXECUTION_FAILED,
                    error_message=(
                        f"Entry script smoke command failed for {step['entry_script']} "
                        f"with return code {result.returncode}: {detail}"
                    ),
                    retriable=False,
                )
            )
        except subprocess.TimeoutExpired as exc:
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_TIMEOUT,
                    error_message=f"Smoke command timed out after {args.timeout_seconds}s: {exc}",
                    retriable=True,
                )
            )
        except Exception as exc:
            step_results.append(
                StepResult(
                    artifact_family=step["artifact_family"],
                    pipeline_job_name=step["pipeline_job_name"],
                    step_name=step["step_name"],
                    status="FAIL",
                    error_code=ERROR_CODE_EXECUTION_FAILED,
                    error_message=str(exc),
                    retriable=False,
                )
            )

    report = {
        "contract_version": CODE_SMOKE_VALIDATE_REPORT_VERSION,
        "artifact_build_id": args.artifact_build_id,
        "status": "FAIL" if any(item.status == "FAIL" for item in step_results) else "PASS",
        "step_results": [result.as_dict() for result in step_results],
    }
    validate_code_smoke_validate_report(report)

    out_path = Path(args.smoke_report_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, sort_keys=True, indent=2), encoding="utf-8")
    return report


def main() -> None:
    args = parse_args()
    try:
        report = validate_smoke(args)
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
