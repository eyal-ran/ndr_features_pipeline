from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from ndr.orchestration.contract_validators import (
    TASK7_ARTIFACT_CONTRACT_ERROR_CODE,
    TASK7_ARTIFACT_GATE_ERROR_CODE,
    TASK7_ARTIFACT_ROLLBACK_ERROR_CODE,
    determine_task7_artifact_rollback_action,
    evaluate_task7_code_artifact_lifecycle_gate,
)
from ndr.scripts import run_code_artifact_validate as validate_script
from ndr.scripts import run_code_bundle_build as build_script
from ndr.scripts import run_code_smoke_validate as smoke_script
from ndr.scripts.run_task7_code_artifact_release_gate import (
    main as run_task7_code_artifact_release_gate_main,
)


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


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _ClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket: str, Key: str, Body, **kwargs):
        if hasattr(Body, "read"):
            payload = Body.read()
        elif isinstance(Body, str):
            payload = Body.encode("utf-8")
        else:
            payload = Body
        self.objects[(Bucket, Key)] = payload

    def head_object(self, *, Bucket: str, Key: str):
        payload = self.objects.get((Bucket, Key))
        if payload is None:
            raise _ClientError("NoSuchKey")
        return {"ContentLength": len(payload)}

    def get_object(self, *, Bucket: str, Key: str):
        payload = self.objects.get((Bucket, Key))
        if payload is None:
            raise _ClientError("NoSuchKey")
        return {"Body": _FakeBody(payload)}


class _FakeTable:
    def __init__(self, items: list[dict]):
        self._items = items

    def query(self, **kwargs):
        return {"Items": self._items}


class _FakeDdbResource:
    def __init__(self, items: list[dict]):
        self._items = items

    def Table(self, name):
        return _FakeTable(self._items)


class _FakeBoto3:
    def __init__(self, *, ddb_items: list[dict], s3_client: _FakeS3Client):
        self._ddb_items = ddb_items
        self._s3_client = s3_client

    def resource(self, **kwargs):
        return _FakeDdbResource(self._ddb_items)

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


def _write_workspace(tmp_path: Path, scripts: dict[str, str]) -> Path:
    scripts_dir = tmp_path / "src" / "ndr" / "scripts"
    scripts_dir.mkdir(parents=True)
    (tmp_path / "src" / "ndr" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "src" / "ndr" / "scripts" / "__init__.py").write_text("", encoding="utf-8")
    for name, body in scripts.items():
        (scripts_dir / name).write_text(body, encoding="utf-8")
    return tmp_path


def _build_args(tmp_path: Path, workspace: Path) -> argparse.Namespace:
    return argparse.Namespace(
        project_name="ndr",
        feature_spec_version="v1",
        artifact_build_id="build-001",
        region_name="us-east-1",
        dpp_config_table_name="dpp_config",
        workspace_root=str(workspace),
        manifest_out=str(tmp_path / "build_manifest.json"),
        artifact_format="tar.gz",
    )


def _validate_args(tmp_path: Path, build_manifest_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        project_name="ndr",
        feature_spec_version="v1",
        artifact_build_id="build-001",
        build_manifest_in=str(build_manifest_path),
        validation_report_out=str(tmp_path / "validation_report.json"),
        region_name="us-east-1",
    )


def _smoke_args(tmp_path: Path, build_manifest_path: Path, validation_report_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        project_name="ndr",
        feature_spec_version="v1",
        artifact_build_id="build-001",
        build_manifest_in=str(build_manifest_path),
        validation_report_in=str(validation_report_path),
        smoke_report_out=str(tmp_path / "smoke_report.json"),
        region_name="us-east-1",
        timeout_seconds=10,
    )


def _task7_gate_evidence() -> dict:
    families = [
        "streaming",
        "dependent",
        "fg_b_baseline",
        "unload",
        "inference",
        "join",
        "training",
        "backfill",
    ]
    return {
        "release_gate": {"block_on_failure": True},
        "lifecycle": {
            "build_status": "PASS",
            "validate_status": "PASS",
            "smoke_status": "PASS",
            "promoted_contract_ready": True,
        },
        "targeted_pytest_matrix": [
            {
                "artifact_family": family,
                "status": "passed",
                "runtime_consumption_verified": True,
                "retry_replay_deterministic": True,
            }
            for family in families
        ],
        "contract_drift": {"check_contract_drift_v3_passed": True},
        "pipeline_definitions": {"checks_passed": True},
        "notebook_checks": {"structure_passed": True, "parity_passed": True},
        "producer_consumer": {"interfaces_verified": True, "drift_detected": False},
        "rollback": {
            "failed_validation_rollback_tested": True,
            "failed_smoke_rollback_tested": True,
            "deterministic_replay_verified": True,
            "playbook_documented": True,
        },
    }


def _family_fixture() -> list[tuple[str, str, str]]:
    return [
        ("pipeline_15m_streaming", "DeltaBuilderStep", "run_delta_builder.py"),
        ("pipeline_15m_dependent", "FGCCorrBuilderStep", "run_fg_c_builder.py"),
        ("pipeline_fg_b_baseline", "FGBaselineBuilderStep", "run_fg_b_builder.py"),
        ("pipeline_machine_inventory_unload", "MachineInventoryUnloadStep", "run_machine_inventory_unload.py"),
        ("pipeline_inference_predictions", "InferencePredictionsStep", "run_inference_predictions.py"),
        ("pipeline_prediction_feature_join", "PredictionFeatureJoinStep", "run_prediction_feature_join.py"),
        ("pipeline_if_training", "IFTrainingStep", "run_if_training.py"),
        ("pipeline_backfill_historical_extractor", "BackfillHistoricalExtractorStep", "run_historical_windows_extractor.py"),
    ]


def test_task7_end_to_end_artifact_lifecycle_integration_is_deterministic(monkeypatch, tmp_path: Path):
    cli_body = "import argparse\nparser = argparse.ArgumentParser(); parser.add_argument('--help-only', action='store_true'); parser.parse_args()\n"
    workspace = _write_workspace(tmp_path, {entry: cli_body for _, _, entry in _family_fixture()})

    ddb_items = [_pipeline_item(job, "v1", step, entry) for job, step, entry in _family_fixture()]
    fake_s3 = _FakeS3Client()
    fake_boto3 = _FakeBoto3(ddb_items=ddb_items, s3_client=fake_s3)

    monkeypatch.setattr(build_script, "_load_boto3", lambda: fake_boto3)
    monkeypatch.setattr(build_script, "_ddb_key", lambda name: _FakeKey(name))
    monkeypatch.setattr(validate_script, "_load_boto3", lambda: fake_boto3)
    monkeypatch.setattr(smoke_script, "_load_boto3", lambda: fake_boto3)

    build_output = build_script.build_and_publish(_build_args(tmp_path, workspace))
    families = {item["artifact_family"] for item in build_output["step_artifacts"]}
    assert families == {
        "streaming",
        "dependent",
        "fg_b_baseline",
        "unload",
        "inference",
        "join",
        "training",
        "backfill",
    }

    manifest_path = tmp_path / "build_manifest.json"
    validation_report = validate_script.validate_artifacts(_validate_args(tmp_path, manifest_path))
    smoke_report = smoke_script.validate_smoke(
        _smoke_args(tmp_path, manifest_path, tmp_path / "validation_report.json")
    )

    assert validation_report["status"] == "PASS"
    assert smoke_report["status"] == "PASS"
    assert len(smoke_report["step_results"]) == 8

    validation_report_replay = validate_script.validate_artifacts(_validate_args(tmp_path, manifest_path))
    smoke_report_replay = smoke_script.validate_smoke(
        _smoke_args(tmp_path, manifest_path, tmp_path / "validation_report.json")
    )
    assert validation_report == validation_report_replay
    assert smoke_report == smoke_report_replay


def test_task7_release_gate_and_cli_are_green_for_complete_evidence(tmp_path: Path, capsys):
    evidence = _task7_gate_evidence()
    report = evaluate_task7_code_artifact_lifecycle_gate(evidence=evidence)
    assert report["status"] == "go"
    assert report["failed_checks"] == []

    evidence_path = tmp_path / "task7_evidence.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    rc = run_task7_code_artifact_release_gate_main(["--evidence-path", str(evidence_path)])
    assert rc == 0
    assert '"status": "go"' in capsys.readouterr().out


def test_task7_release_gate_fails_fast_when_family_coverage_is_incomplete():
    evidence = _task7_gate_evidence()
    evidence["targeted_pytest_matrix"] = [
        row for row in evidence["targeted_pytest_matrix"] if row["artifact_family"] != "join"
    ]

    with pytest.raises(ValueError, match=TASK7_ARTIFACT_GATE_ERROR_CODE):
        evaluate_task7_code_artifact_lifecycle_gate(evidence=evidence)


def test_task7_release_gate_contract_shape_violation_is_rejected():
    with pytest.raises(ValueError, match=TASK7_ARTIFACT_CONTRACT_ERROR_CODE):
        evaluate_task7_code_artifact_lifecycle_gate(
            evidence={"release_gate": {}, "lifecycle": {}, "targeted_pytest_matrix": []}
        )


def test_task7_negative_validation_failure_has_deterministic_rollback(monkeypatch, tmp_path: Path):
    workspace = _write_workspace(
        tmp_path,
        {"run_delta_builder.py": "import argparse\nargparse.ArgumentParser().parse_args()\n"},
    )
    ddb_items = [
        _pipeline_item("pipeline_15m_streaming", "v1", "DeltaBuilderStep", "run_delta_builder.py"),
    ]
    fake_s3 = _FakeS3Client()
    fake_boto3 = _FakeBoto3(ddb_items=ddb_items, s3_client=fake_s3)

    monkeypatch.setattr(build_script, "_load_boto3", lambda: fake_boto3)
    monkeypatch.setattr(build_script, "_ddb_key", lambda name: _FakeKey(name))
    monkeypatch.setattr(validate_script, "_load_boto3", lambda: fake_boto3)

    build_output = build_script.build_and_publish(_build_args(tmp_path, workspace))
    artifact_uri = build_output["step_artifacts"][0]["code_artifact_s3_uri"]
    bucket, key = build_script._split_s3_uri(artifact_uri)
    fake_s3.objects[(bucket, key)] = b"corrupted-bytes"

    report = validate_script.validate_artifacts(_validate_args(tmp_path, tmp_path / "build_manifest.json"))
    assert report["status"] == "FAIL"
    assert report["step_results"][0]["error_code"] == validate_script.ERROR_CODE_HASH_MISMATCH

    rollback = determine_task7_artifact_rollback_action(
        failure_stage="validate",
        reason_code=validate_script.ERROR_CODE_HASH_MISMATCH,
    )
    assert rollback["error_code"] == TASK7_ARTIFACT_ROLLBACK_ERROR_CODE
    assert rollback["failure_stage"] == "validate"
    assert "block_promotion" in rollback["actions"]


def test_task7_negative_smoke_failure_has_deterministic_rollback(monkeypatch, tmp_path: Path):
    workspace = _write_workspace(
        tmp_path,
        {
            "run_inference_predictions.py": "raise RuntimeError('boom')\n",
        },
    )
    ddb_items = [
        _pipeline_item(
            "pipeline_inference_predictions",
            "v1",
            "InferencePredictionsStep",
            "run_inference_predictions.py",
        ),
    ]
    fake_s3 = _FakeS3Client()
    fake_boto3 = _FakeBoto3(ddb_items=ddb_items, s3_client=fake_s3)

    monkeypatch.setattr(build_script, "_load_boto3", lambda: fake_boto3)
    monkeypatch.setattr(build_script, "_ddb_key", lambda name: _FakeKey(name))
    monkeypatch.setattr(validate_script, "_load_boto3", lambda: fake_boto3)
    monkeypatch.setattr(smoke_script, "_load_boto3", lambda: fake_boto3)

    build_script.build_and_publish(_build_args(tmp_path, workspace))
    validate_report = validate_script.validate_artifacts(_validate_args(tmp_path, tmp_path / "build_manifest.json"))
    smoke_report = smoke_script.validate_smoke(
        _smoke_args(tmp_path, tmp_path / "build_manifest.json", tmp_path / "validation_report.json")
    )

    assert validate_report["status"] == "PASS"
    assert smoke_report["status"] == "FAIL"
    assert smoke_report["step_results"][0]["error_code"] == smoke_script.ERROR_CODE_EXECUTION_FAILED

    rollback = determine_task7_artifact_rollback_action(
        failure_stage="smoke",
        reason_code=smoke_script.ERROR_CODE_EXECUTION_FAILED,
    )
    assert rollback["status"] == "ROLLBACK_REQUIRED"
    assert "rollback_promoted_contract_pointers" in rollback["actions"]
