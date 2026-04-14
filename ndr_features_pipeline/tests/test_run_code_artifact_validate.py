from __future__ import annotations

import argparse
import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from ndr.orchestration.remediation_contracts import ContractValidationError
from ndr.scripts import run_code_artifact_validate as script


class _ClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str], bytes]):
        self._objects = objects

    def head_object(self, *, Bucket: str, Key: str):
        obj = self._objects.get((Bucket, Key))
        if obj is None:
            raise _ClientError("NoSuchKey")
        return {"ContentLength": len(obj)}

    def get_object(self, *, Bucket: str, Key: str):
        obj = self._objects.get((Bucket, Key))
        if obj is None:
            raise _ClientError("NoSuchKey")
        return {"Body": _FakeBody(obj)}


class _FakeBoto3:
    def __init__(self, s3_client: _FakeS3Client):
        self._s3_client = s3_client

    def client(self, **kwargs):
        return self._s3_client


def _make_tar(entry_script: str = "src/ndr/scripts/run_delta_builder.py") -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=buffer) as tf:
        content = b"print('ok')\n"
        info = tarfile.TarInfo(name=entry_script)
        info.size = len(content)
        tf.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def _manifest(*, sha: str, uri: str, entry_script: str = "src/ndr/scripts/run_delta_builder.py") -> dict:
    return {
        "contract_version": "code_bundle_build_output.v1",
        "project_name": "proj",
        "feature_spec_version": "v1",
        "artifact_build_id": "build-1",
        "artifact_sha256": sha,
        "artifact_format": "tar.gz",
        "step_artifacts": [
            {
                "artifact_family": "streaming",
                "pipeline_job_name": "pipeline_15m_streaming",
                "step_name": "DeltaBuilderStep",
                "entry_script": entry_script,
                "code_artifact_s3_uri": uri,
                "artifact_build_id": "build-1",
                "artifact_sha256": sha,
                "artifact_format": "tar.gz",
            }
        ],
    }


def _args(tmp_path: Path, manifest_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        project_name="proj",
        feature_spec_version="v1",
        artifact_build_id="build-1",
        build_manifest_in=str(manifest_path),
        validation_report_out=str(tmp_path / "report.json"),
        region_name="us-east-1",
    )


def test_validate_artifacts_positive(monkeypatch, tmp_path: Path):
    uri = "s3://bucket/code/artifacts/build-1/source.tar.gz"
    payload = _make_tar()
    sha = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(sha=sha, uri=uri)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): payload})))
    report = script.validate_artifacts(_args(tmp_path, manifest_path))

    assert report["status"] == "PASS"
    assert report["validated_steps"] == 1
    assert report["step_results"][0]["error_code"] == "OK"


def test_validate_artifacts_missing_object_fails_fast(monkeypatch, tmp_path: Path):
    uri = "s3://bucket/code/artifacts/build-1/source.tar.gz"
    payload = _make_tar()
    sha = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(sha=sha, uri=uri)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3(_FakeS3Client({})))
    report = script.validate_artifacts(_args(tmp_path, manifest_path))

    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == script.ERROR_CODE_OBJECT_MISSING


def test_validate_artifacts_hash_mismatch(monkeypatch, tmp_path: Path):
    uri = "s3://bucket/code/artifacts/build-1/source.tar.gz"
    payload = _make_tar()
    manifest = _manifest(sha="a" * 64, uri=uri)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): payload})))
    report = script.validate_artifacts(_args(tmp_path, manifest_path))

    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == script.ERROR_CODE_HASH_MISMATCH


def test_validate_artifacts_wrong_format(monkeypatch, tmp_path: Path):
    uri = "s3://bucket/code/artifacts/build-1/source.tar.gz"
    payload = b"not-a-tar"
    sha = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(sha=sha, uri=uri)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): payload})))
    report = script.validate_artifacts(_args(tmp_path, manifest_path))

    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == script.ERROR_CODE_ARCHIVE_INVALID


def test_validate_artifacts_missing_entry_script(monkeypatch, tmp_path: Path):
    uri = "s3://bucket/code/artifacts/build-1/source.tar.gz"
    payload = _make_tar(entry_script="src/ndr/scripts/run_other.py")
    sha = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(sha=sha, uri=uri, entry_script="src/ndr/scripts/run_delta_builder.py")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): payload})))
    report = script.validate_artifacts(_args(tmp_path, manifest_path))

    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == script.ERROR_CODE_ENTRY_SCRIPT_MISSING


def test_validate_artifacts_schema_invalid_contract(monkeypatch, tmp_path: Path):
    uri = "s3://bucket/code/artifacts/build-1/source.tar.gz"
    payload = _make_tar()
    sha = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(sha=sha, uri=uri)
    manifest["step_artifacts"][0]["pipeline_job_name"] = "pipeline_unknown"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3(_FakeS3Client({("bucket", "code/artifacts/build-1/source.tar.gz"): payload})))
    with pytest.raises(ContractValidationError, match="CONTRACT_INVALID_FIELD"):
        script.validate_artifacts(_args(tmp_path, manifest_path))
