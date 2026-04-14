from __future__ import annotations

import argparse
import importlib
import gzip
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ndr.orchestration.code_artifact_contracts import (
    CODE_BUNDLE_BUILD_OUTPUT_VERSION,
    validate_code_bundle_build_output,
)

DEFAULT_MANIFEST_OUT = "/tmp/code_bundle_build_output.json"
DEFAULT_ARTIFACT_FORMAT = "tar.gz"
DEFAULT_ARCHIVE_NAME = "source.tar.gz"
ERROR_CODE_CONTRACT = "CODE_BUNDLE_CONTRACT_VIOLATION"
ERROR_CODE_PLACEHOLDER = "CODE_BUNDLE_PLACEHOLDER_ARGUMENT"
ERROR_CODE_DISCOVERY = "CODE_BUNDLE_DISCOVERY_AMBIGUOUS"
ERROR_CODE_ENTRY_SCRIPT = "CODE_BUNDLE_ENTRY_SCRIPT_MISSING"
ERROR_CODE_UPLOAD = "CODE_BUNDLE_UPLOAD_FAILED"

FORBIDDEN_MARKERS = ("<required:", "<placeholder", "${", "env_fallback", "code_default")
PIPELINE_ARTIFACT_FAMILY: dict[str, str] = {
    "pipeline_15m_streaming": "streaming",
    "pipeline_15m_dependent": "dependent",
    "pipeline_fg_b_baseline": "fg_b_baseline",
    "pipeline_machine_inventory_unload": "unload",
    "pipeline_inference_predictions": "inference",
    "pipeline_prediction_feature_join": "join",
    "pipeline_if_training": "training",
    "pipeline_backfill_historical_extractor": "backfill",
    "pipeline_backfill_15m_reprocessing": "backfill",
}


class CodeBundleBuildError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class StepContract:
    artifact_family: str
    pipeline_job_name: str
    step_name: str
    code_prefix_s3: str
    entry_script: str



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build deterministic code bundle artifacts for NDR deployment")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--artifact-build-id", required=True)
    parser.add_argument("--region-name")
    parser.add_argument("--dpp-config-table-name")
    parser.add_argument("--workspace-root", default=os.getcwd())
    parser.add_argument("--manifest-out", default=DEFAULT_MANIFEST_OUT)
    parser.add_argument("--artifact-format", default=DEFAULT_ARTIFACT_FORMAT)
    return parser.parse_args()


def _is_placeholder(value: str) -> bool:
    lowered = value.strip().lower()
    return any(marker in lowered for marker in FORBIDDEN_MARKERS)


def _validate_inputs(args: argparse.Namespace) -> None:
    for field in ("project_name", "feature_spec_version", "artifact_build_id"):
        value = str(getattr(args, field, "") or "").strip()
        if not value:
            raise CodeBundleBuildError(ERROR_CODE_CONTRACT, f"{field} must be a non-empty string")
        if _is_placeholder(value):
            raise CodeBundleBuildError(ERROR_CODE_PLACEHOLDER, f"{field} contains placeholder markers: {value!r}")
    if args.artifact_format != DEFAULT_ARTIFACT_FORMAT:
        raise CodeBundleBuildError(
            ERROR_CODE_CONTRACT,
            f"artifact_format must be '{DEFAULT_ARTIFACT_FORMAT}', got {args.artifact_format!r}",
        )


def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise CodeBundleBuildError(ERROR_CODE_CONTRACT, f"Invalid s3 uri: {uri!r}")
    no_scheme = uri[5:]
    bucket, _, key = no_scheme.partition("/")
    if not bucket or not key:
        raise CodeBundleBuildError(ERROR_CODE_CONTRACT, f"Invalid s3 uri: {uri!r}")
    return bucket, key


def _load_boto3() -> Any:
    return importlib.import_module("boto3")


def _ddb_key(name: str) -> Any:
    module = importlib.import_module("boto3.dynamodb.conditions")
    return module.Key(name)


def discover_step_contracts(
    *,
    project_name: str,
    feature_spec_version: str,
    table_name: str | None,
    region_name: str | None,
) -> list[StepContract]:
    boto3 = _load_boto3()
    key = _ddb_key
    resource_kwargs: dict[str, Any] = {"service_name": "dynamodb"}
    if region_name:
        resource_kwargs["region_name"] = region_name
    ddb = boto3.resource(**resource_kwargs)
    resolved_table_name = table_name or os.getenv("DPP_CONFIG_TABLE_NAME") or "dpp_config"
    table = ddb.Table(resolved_table_name)

    items: list[dict[str, Any]] = []
    query_kwargs: dict[str, Any] = {
        "KeyConditionExpression": key("project_name").eq(project_name)
        & key("job_name_version").begins_with("pipeline_"),
    }
    while True:
        response = table.query(**query_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        query_kwargs["ExclusiveStartKey"] = last_key

    contracts: list[StepContract] = []
    for item in items:
        job_name_version = str(item.get("job_name_version") or "")
        job_name, _, version = job_name_version.partition("#")
        if version != feature_spec_version:
            continue
        if job_name not in PIPELINE_ARTIFACT_FAMILY:
            continue
        spec = item.get("spec")
        if not isinstance(spec, dict):
            raise CodeBundleBuildError(
                ERROR_CODE_DISCOVERY,
                f"{job_name_version} missing spec map for step discovery",
            )
        steps = ((spec.get("scripts") or {}).get("steps") or {})
        if not isinstance(steps, dict) or not steps:
            raise CodeBundleBuildError(
                ERROR_CODE_DISCOVERY,
                f"{job_name_version} missing scripts.steps map for step discovery",
            )
        for step_name in sorted(steps):
            step_spec = steps[step_name]
            if not isinstance(step_spec, dict):
                raise CodeBundleBuildError(
                    ERROR_CODE_DISCOVERY,
                    f"{job_name_version}.{step_name} must be an object",
                )
            code_prefix_s3 = str(step_spec.get("code_prefix_s3") or "").strip()
            entry_script = str(step_spec.get("entry_script") or "").strip()
            if not code_prefix_s3 or not entry_script:
                raise CodeBundleBuildError(
                    ERROR_CODE_DISCOVERY,
                    f"{job_name_version}.{step_name} requires code_prefix_s3 and entry_script",
                )
            if _is_placeholder(code_prefix_s3) or _is_placeholder(entry_script):
                raise CodeBundleBuildError(
                    ERROR_CODE_DISCOVERY,
                    f"{job_name_version}.{step_name} still contains placeholder values",
                )
            contracts.append(
                StepContract(
                    artifact_family=PIPELINE_ARTIFACT_FAMILY[job_name],
                    pipeline_job_name=job_name,
                    step_name=step_name,
                    code_prefix_s3=code_prefix_s3,
                    entry_script=entry_script,
                )
            )

    if not contracts:
        raise CodeBundleBuildError(
            ERROR_CODE_DISCOVERY,
            "No pipeline step contracts discovered. Verify DDB seeds and feature_spec_version.",
        )

    seen: set[tuple[str, str]] = set()
    for contract in contracts:
        key = (contract.pipeline_job_name, contract.step_name)
        if key in seen:
            raise CodeBundleBuildError(
                ERROR_CODE_DISCOVERY,
                f"Ambiguous duplicate step contract discovered for {contract.pipeline_job_name}.{contract.step_name}",
            )
        seen.add(key)

    return sorted(contracts, key=lambda c: (c.pipeline_job_name, c.step_name))


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stage_source_tree(*, workspace_root: Path, step_contracts: list[StepContract]) -> Path:
    if not workspace_root.exists():
        raise CodeBundleBuildError(ERROR_CODE_CONTRACT, f"workspace_root does not exist: {workspace_root}")
    source_root = workspace_root / "src" / "ndr"
    if not source_root.exists():
        raise CodeBundleBuildError(ERROR_CODE_CONTRACT, f"Missing source tree: {source_root}")

    stage_dir = Path(tempfile.mkdtemp(prefix="ndr-code-bundle-"))
    target_src = stage_dir / "src" / "ndr"
    shutil.copytree(source_root, target_src)

    scripts_root = source_root / "scripts"
    for contract in step_contracts:
        source_script = scripts_root / contract.entry_script
        if not source_script.exists():
            raise CodeBundleBuildError(
                ERROR_CODE_ENTRY_SCRIPT,
                f"Resolved entry_script for {contract.pipeline_job_name}.{contract.step_name} is missing: {source_script}",
            )
        shutil.copy2(source_script, stage_dir / contract.entry_script)

    return stage_dir


def _collect_files(root_dir: Path) -> list[Path]:
    files = [path for path in root_dir.rglob("*") if path.is_file()]
    return sorted(files, key=lambda path: path.relative_to(root_dir).as_posix())


def build_internal_manifest(
    *, project_name: str, feature_spec_version: str, artifact_build_id: str, stage_dir: Path, step_contracts: list[StepContract]
) -> dict[str, Any]:
    files = _collect_files(stage_dir)
    return {
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "artifact_build_id": artifact_build_id,
        "steps": [
            {
                "artifact_family": c.artifact_family,
                "pipeline_job_name": c.pipeline_job_name,
                "step_name": c.step_name,
                "entry_script": c.entry_script,
            }
            for c in step_contracts
        ],
        "files": [
            {
                "path": path.relative_to(stage_dir).as_posix(),
                "sha256": _hash_file(path),
                "size_bytes": path.stat().st_size,
            }
            for path in files
        ],
    }


def create_deterministic_tar_gz(*, source_dir: Path, archive_path: Path) -> None:
    files = _collect_files(source_dir)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with archive_path.open("wb") as raw_out:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_out, mtime=0) as gz_out:
            with tarfile.open(mode="w", fileobj=gz_out, format=tarfile.PAX_FORMAT) as tar:
                for file_path in files:
                    rel = file_path.relative_to(source_dir).as_posix()
                    tar_info = tar.gettarinfo(str(file_path), arcname=rel)
                    tar_info.mtime = 0
                    tar_info.uid = 0
                    tar_info.gid = 0
                    tar_info.uname = ""
                    tar_info.gname = ""
                    with file_path.open("rb") as handle:
                        tar.addfile(tar_info, fileobj=handle)


def _s3_put_text(*, s3_client: Any, uri: str, text: str) -> None:
    bucket, key = _split_s3_uri(uri)
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType="text/plain")
    except Exception as exc:
        raise CodeBundleBuildError(ERROR_CODE_UPLOAD, f"Failed to upload sidecar text to {uri}: {exc}") from exc


def _s3_put_json(*, s3_client: Any, uri: str, payload: dict[str, Any]) -> None:
    bucket, key = _split_s3_uri(uri)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=encoded, ContentType="application/json")
    except Exception as exc:
        raise CodeBundleBuildError(ERROR_CODE_UPLOAD, f"Failed to upload manifest sidecar to {uri}: {exc}") from exc


def _s3_put_file(*, s3_client: Any, local_path: Path, uri: str) -> None:
    bucket, key = _split_s3_uri(uri)
    with local_path.open("rb") as handle:
        try:
            s3_client.put_object(Bucket=bucket, Key=key, Body=handle.read(), ContentType="application/gzip")
        except Exception as exc:
            raise CodeBundleBuildError(ERROR_CODE_UPLOAD, f"Failed to upload artifact to {uri}: {exc}") from exc


def build_and_publish(args: argparse.Namespace) -> dict[str, Any]:
    boto3 = _load_boto3()
    _validate_inputs(args)

    contracts = discover_step_contracts(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        table_name=args.dpp_config_table_name,
        region_name=args.region_name,
    )
    stage_dir = stage_source_tree(workspace_root=Path(args.workspace_root), step_contracts=contracts)
    internal_manifest = build_internal_manifest(
        project_name=args.project_name,
        feature_spec_version=args.feature_spec_version,
        artifact_build_id=args.artifact_build_id,
        stage_dir=stage_dir,
        step_contracts=contracts,
    )
    (stage_dir / "MANIFEST.json").write_text(
        json.dumps(internal_manifest, sort_keys=True, indent=2),
        encoding="utf-8",
    )

    archive_path = Path(tempfile.mkdtemp(prefix="ndr-code-archive-")) / DEFAULT_ARCHIVE_NAME
    create_deterministic_tar_gz(source_dir=stage_dir, archive_path=archive_path)
    artifact_sha256 = _hash_file(archive_path)

    s3_kwargs: dict[str, Any] = {"service_name": "s3"}
    if args.region_name:
        s3_kwargs["region_name"] = args.region_name
    s3_client = boto3.client(**s3_kwargs)

    step_artifacts: list[dict[str, Any]] = []
    for contract in contracts:
        artifact_uri = (
            f"{contract.code_prefix_s3.rstrip('/')}/artifacts/{args.artifact_build_id}/{DEFAULT_ARCHIVE_NAME}"
        )
        _s3_put_file(s3_client=s3_client, local_path=archive_path, uri=artifact_uri)
        _s3_put_text(s3_client=s3_client, uri=artifact_uri.replace(DEFAULT_ARCHIVE_NAME, "source.tar.gz.sha256"), text=artifact_sha256)
        _s3_put_json(s3_client=s3_client, uri=artifact_uri.replace(DEFAULT_ARCHIVE_NAME, "manifest.json"), payload=internal_manifest)

        step_artifacts.append(
            {
                "artifact_family": contract.artifact_family,
                "pipeline_job_name": contract.pipeline_job_name,
                "step_name": contract.step_name,
                "entry_script": contract.entry_script,
                "code_artifact_s3_uri": artifact_uri,
                "artifact_build_id": args.artifact_build_id,
                "artifact_sha256": artifact_sha256,
                "artifact_format": DEFAULT_ARTIFACT_FORMAT,
            }
        )

    output = {
        "contract_version": CODE_BUNDLE_BUILD_OUTPUT_VERSION,
        "project_name": args.project_name,
        "feature_spec_version": args.feature_spec_version,
        "artifact_build_id": args.artifact_build_id,
        "artifact_sha256": artifact_sha256,
        "artifact_format": DEFAULT_ARTIFACT_FORMAT,
        "step_artifacts": step_artifacts,
    }
    validate_code_bundle_build_output(output)

    manifest_out = Path(args.manifest_out)
    manifest_out.parent.mkdir(parents=True, exist_ok=True)
    manifest_out.write_text(json.dumps(output, sort_keys=True, indent=2), encoding="utf-8")
    return output


def main() -> None:
    args = parse_args()
    try:
        output = build_and_publish(args)
    except CodeBundleBuildError as exc:
        print(json.dumps({"status": "FAIL", "error_code": exc.code, "message": str(exc)}, sort_keys=True), file=sys.stderr)
        raise SystemExit(2) from exc
    print(json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    main()
