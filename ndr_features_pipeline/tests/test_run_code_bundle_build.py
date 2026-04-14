from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pytest

from ndr.scripts import run_code_bundle_build as script


class _FakeTable:
    def __init__(self, pages):
        self._pages = list(pages)

    def query(self, **kwargs):
        if kwargs.get("ExclusiveStartKey") == {"cursor": "p2"}:
            return self._pages[1]
        return self._pages[0]


class _FakeDdbResource:
    def __init__(self, pages):
        self._pages = pages

    def Table(self, name):
        return _FakeTable(self._pages)


class _FakeS3Client:
    def __init__(self):
        self.uploads: list[dict[str, str]] = []

    def put_object(self, **kwargs):
        self.uploads.append(kwargs)


class _Expr:
    def __and__(self, other):
        return self


class _FakeKey:
    def __init__(self, name: str):
        self.name = name

    def eq(self, value):
        return _Expr()

    def begins_with(self, value):
        return _Expr()


class _FakeBoto3:
    def __init__(self, ddb_pages, s3_client=None):
        self._ddb_pages = ddb_pages
        self._s3_client = s3_client or _FakeS3Client()

    def resource(self, **kwargs):
        return _FakeDdbResource(self._ddb_pages)

    def client(self, **kwargs):
        return self._s3_client


def _pipeline_item(job_name: str, version: str, step_name: str, entry_script: str) -> dict:
    return {
        "job_name_version": f"{job_name}#{version}",
        "spec": {
            "scripts": {
                "steps": {
                    step_name: {
                        "code_prefix_s3": f"s3://bucket/{job_name}/{step_name}/",
                        "entry_script": entry_script,
                    }
                }
            }
        },
    }


def _write_workspace(tmp_path: Path, script_names: list[str]) -> Path:
    scripts_dir = tmp_path / "src" / "ndr" / "scripts"
    scripts_dir.mkdir(parents=True)
    (tmp_path / "src" / "ndr" / "__init__.py").write_text("", encoding="utf-8")
    for name in script_names:
        (scripts_dir / name).write_text("print('ok')\n", encoding="utf-8")
    return tmp_path


def test_create_deterministic_tar_gz_is_hash_stable(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("alpha\n", encoding="utf-8")
    (source / "b.txt").write_text("beta\n", encoding="utf-8")

    one = tmp_path / "one.tar.gz"
    two = tmp_path / "two.tar.gz"
    script.create_deterministic_tar_gz(source_dir=source, archive_path=one)
    script.create_deterministic_tar_gz(source_dir=source, archive_path=two)

    digest_one = hashlib.sha256(one.read_bytes()).hexdigest()
    digest_two = hashlib.sha256(two.read_bytes()).hexdigest()
    assert digest_one == digest_two


def test_discover_step_contracts_covers_all_pipeline_families(monkeypatch):
    all_job_names = sorted(script.PIPELINE_ARTIFACT_FAMILY)
    first_page = {
        "Items": [_pipeline_item(name, "v1", "StepA", "run_delta_builder.py") for name in all_job_names[:5]],
        "LastEvaluatedKey": {"cursor": "p2"},
    }
    second_page = {
        "Items": [_pipeline_item(name, "v1", "StepB", "run_fg_a_builder.py") for name in all_job_names[5:]],
    }

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3([first_page, second_page]))
    monkeypatch.setattr(script, "_ddb_key", lambda name: _FakeKey(name))
    contracts = script.discover_step_contracts(
        project_name="ndr",
        feature_spec_version="v1",
        table_name="dpp_config",
        region_name="us-east-1",
    )

    discovered = {c.pipeline_job_name for c in contracts}
    assert discovered == set(all_job_names)


def test_build_and_publish_uploads_artifact_and_sidecars(monkeypatch, tmp_path: Path):
    workspace = _write_workspace(tmp_path, ["run_delta_builder.py"])
    ddb_pages = {
        "Items": [
            _pipeline_item("pipeline_15m_streaming", "v1", "DeltaBuilderStep", "run_delta_builder.py"),
        ]
    }
    fake_s3 = _FakeS3Client()

    monkeypatch.setattr(script, "_load_boto3", lambda: _FakeBoto3([ddb_pages], s3_client=fake_s3))
    monkeypatch.setattr(script, "_ddb_key", lambda name: _FakeKey(name))

    manifest_path = tmp_path / "out" / "manifest.json"
    args = argparse.Namespace(
        project_name="ndr",
        feature_spec_version="v1",
        artifact_build_id="build-001",
        region_name="us-east-1",
        dpp_config_table_name="dpp_config",
        workspace_root=str(workspace),
        manifest_out=str(manifest_path),
        artifact_format="tar.gz",
    )

    output = script.build_and_publish(args)
    assert manifest_path.exists()
    saved = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert saved["artifact_build_id"] == "build-001"
    assert output["step_artifacts"][0]["code_artifact_s3_uri"].endswith("/artifacts/build-001/source.tar.gz")

    keys = sorted(f"{u['Bucket']}/{u['Key']}" for u in fake_s3.uploads)
    assert any(k.endswith("/source.tar.gz") for k in keys)
    assert any(k.endswith("/source.tar.gz.sha256") for k in keys)
    assert any(k.endswith("/manifest.json") for k in keys)


def test_validate_inputs_rejects_placeholder_values():
    args = argparse.Namespace(
        project_name="ndr",
        feature_spec_version="v1",
        artifact_build_id="<required:ArtifactBuildId>",
        artifact_format="tar.gz",
    )
    with pytest.raises(script.CodeBundleBuildError) as exc:
        script._validate_inputs(args)
    assert exc.value.code == script.ERROR_CODE_PLACEHOLDER
