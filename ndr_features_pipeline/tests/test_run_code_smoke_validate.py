from __future__ import annotations

import argparse
import io
import json
import tarfile
from pathlib import Path

from ndr.scripts import run_code_smoke_validate as script


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes]):
        self._objects = objects

    def get_object(self, *, Bucket: str, Key: str):
        obj = self._objects.get((Bucket, Key))
        if obj is None:
            raise FileNotFoundError(f"Missing object {Bucket}/{Key}")
        return {"Body": _FakeBody(obj)}


class _FakeBoto3:
    def __init__(self, s3_client: _FakeS3Client):
        self._s3_client = s3_client

    def client(self, **kwargs):
        return self._s3_client


def _manifest(*, entry_script: str) -> dict:
    return {
        "contract_version": "code_bundle_build_output.v1",
        "project_name": "proj",
        "feature_spec_version": "v1",
        "artifact_build_id": "build-1",
        "artifact_sha256": "a" * 64,
        "artifact_format": "tar.gz",
        "step_artifacts": [
            {
                "artifact_family": "streaming",
                "pipeline_job_name": "pipeline_15m_streaming",
                "step_name": "DeltaBuilderStep",
                "entry_script": entry_script,
                "code_artifact_s3_uri": "s3://bucket/code/artifacts/build-1/source.tar.gz",
                "artifact_build_id": "build-1",
                "artifact_sha256": "a" * 64,
                "artifact_format": "tar.gz",
            }
        ],
    }


def _args(tmp_path: Path, manifest_path: Path) -> argparse.Namespace:
    validation_report_path = tmp_path / "validation_report.json"
    validation_report_path.write_text(
        json.dumps(
            {
                "contract_version": "code_artifact_validate_report.v1",
                "artifact_build_id": "build-1",
                "status": "PASS",
                "validated_steps": 1,
                "step_results": [
                    {
                        "artifact_family": "streaming",
                        "pipeline_job_name": "pipeline_15m_streaming",
                        "step_name": "DeltaBuilderStep",
                        "status": "PASS",
                        "error_code": "OK",
                        "error_message": "",
                        "retriable": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return argparse.Namespace(
        project_name="proj",
        feature_spec_version="v1",
        artifact_build_id="build-1",
        build_manifest_in=str(manifest_path),
        validation_report_in=str(validation_report_path),
        smoke_report_out=str(tmp_path / "smoke.json"),
        region_name="us-east-1",
        timeout_seconds=5,
    )


def _make_tar(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=buffer) as tf:
        for path, content in files.items():
            payload = content.encode("utf-8")
            info = tarfile.TarInfo(name=path)
            info.size = len(payload)
            tf.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


def test_validate_smoke_positive(monkeypatch, tmp_path: Path):
    artifact = _make_tar(
        {
            "run_step.py": "import argparse\nparser = argparse.ArgumentParser(); parser.parse_args()\n",
            "src/ndr/__init__.py": "",
        }
    )
    manifest = _manifest(entry_script="run_step.py")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(
        script,
        "_load_boto3",
        lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): artifact})),
    )

    report = script.validate_smoke(_args(tmp_path, manifest_path))

    assert report["status"] == "PASS"
    assert report["step_results"][0]["error_code"] == "OK"


def test_validate_smoke_broken_entry_script_reports_execution_failure(monkeypatch, tmp_path: Path):
    artifact = _make_tar(
        {
            "run_step.py": "raise RuntimeError('boom')\n",
            "src/ndr/__init__.py": "",
        }
    )
    manifest = _manifest(entry_script="run_step.py")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(
        script,
        "_load_boto3",
        lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): artifact})),
    )

    report = script.validate_smoke(_args(tmp_path, manifest_path))

    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == script.ERROR_CODE_EXECUTION_FAILED


def test_validate_smoke_import_failure_has_deterministic_code(monkeypatch, tmp_path: Path):
    artifact = _make_tar({"run_step.py": "import missing_module\n"})
    manifest = _manifest(entry_script="run_step.py")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(
        script,
        "_load_boto3",
        lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): artifact})),
    )

    report = script.validate_smoke(_args(tmp_path, manifest_path))

    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == script.ERROR_CODE_IMPORT_FAILED


def test_validate_smoke_schema_compliance_rejects_manifest_mismatch(tmp_path: Path):
    manifest = _manifest(entry_script="run_step.py")
    manifest["artifact_build_id"] = "another"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    try:
        script.validate_smoke(_args(tmp_path, manifest_path))
    except ValueError as exc:
        assert "artifact_build_id mismatch" in str(exc)
    else:
        raise AssertionError("Expected a contract mismatch ValueError")


def test_validate_smoke_blocks_when_validation_report_not_pass(tmp_path: Path):
    manifest = _manifest(entry_script="run_step.py")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    args = _args(tmp_path, manifest_path)
    validation_report_path = Path(args.validation_report_in)
    validation_report = json.loads(validation_report_path.read_text(encoding="utf-8"))
    validation_report["status"] = "FAIL"
    validation_report_path.write_text(json.dumps(validation_report), encoding="utf-8")

    try:
        script.validate_smoke(args)
    except ValueError as exc:
        assert script.ERROR_CODE_VALIDATE_BLOCKED in str(exc)
    else:
        raise AssertionError("Expected smoke validation to block on failed artifact validation")


def test_build_smoke_env_adds_extracted_src_prefix(tmp_path: Path):
    extract_dir = tmp_path / "extract"
    (extract_dir / "src").mkdir(parents=True)

    env = script._build_smoke_env(extract_dir=extract_dir)

    assert str((extract_dir / "src").resolve()) in env["PYTHONPATH"].split(":")[0]
