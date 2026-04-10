import json
import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *_args, **_kwargs: None
dynamodb_module = types.ModuleType("boto3.dynamodb")
conditions_module = types.ModuleType("boto3.dynamodb.conditions")
conditions_module.Key = object
sys.modules.setdefault("boto3", boto3_stub)
sys.modules.setdefault("boto3.dynamodb", dynamodb_module)
sys.modules.setdefault("boto3.dynamodb.conditions", conditions_module)

from ndr.orchestration.backfill_contracts import build_family_range_plan
from ndr.processing.historical_windows_extractor_job import _derive_fg_b_reference_ranges
from ndr.scripts.run_backfill_reprocessing_executor import main


def test_historical_extractor_family_ranges_derive_daily_fgb_windows_deterministically():
    rows = [
        {"batch_end_ts_iso": "2025-01-01T00:15:00Z"},
        {"batch_end_ts_iso": "2025-01-01T23:59:00Z"},
        {"batch_end_ts_iso": "2025-01-02T00:01:00Z"},
    ]
    assert _derive_fg_b_reference_ranges(rows) == [
        {"start_ts": "2025-01-01T00:00:00Z", "end_ts": "2025-01-01T23:59:59Z"},
        {"start_ts": "2025-01-02T00:00:00Z", "end_ts": "2025-01-02T23:59:59Z"},
    ]


def test_planner_executes_fgb_without_15m_dependencies_for_fgb_only_scenario():
    plan = build_family_range_plan(
        family_ranges={
            "delta": [],
            "fg_a": [],
            "pair_counts": [],
            "fg_b_baseline": [{"start_ts": "2025-01-01T00:00:00Z", "end_ts": "2025-01-01T23:59:59Z"}],
            "fg_c": [],
        }
    )
    entry = next(item for item in plan if item.family == "fg_b_baseline")
    assert entry.execute is True
    assert entry.reason == "missing_ranges_detected"


def test_executor_completion_payload_includes_fgb_results_for_mixed_and_non_fgb(monkeypatch, capsys):
    import ndr.scripts.run_backfill_reprocessing_executor as module
    import ndr.orchestration.backfill_family_dispatcher as dispatcher_module

    class _SageMaker:
        def __init__(self):
            self.calls = []

        def start_pipeline_execution(self, **kwargs):
            self.calls.append(kwargs)
            return {"PipelineExecutionArn": "arn:aws:sagemaker:us-east-1:123:pipeline-execution/fgb"}

    fake_sm = _SageMaker()
    monkeypatch.setattr(
        module,
        "load_project_parameters",
        lambda **_kwargs: {
            "orchestration_targets": {
                "delta": "pipeline-delta",
                "fg_a": "pipeline-fga",
                "pair_counts": "pipeline-pc",
                "fg_b_baseline": "pipeline-fgb",
                "fg_c": "pipeline-fgc",
            }
        },
    )
    monkeypatch.setattr(dispatcher_module.boto3, "client", lambda name, **_kwargs: fake_sm if name == "sagemaker" else None)

    rc = main(
        [
            "--project-name",
            "ndr-prod",
            "--feature-spec-version",
            "v1",
            "--artifact-family",
            "fg_b_baseline,fg_a",
            "--range-start-ts-iso",
            "2025-01-01T00:00:00Z",
            "--range-end-ts-iso",
            "2025-01-01T23:59:59Z",
        ]
    )
    assert rc == 0
    mixed_payload = json.loads(capsys.readouterr().out.strip())
    assert mixed_payload["fg_b_baseline_results"][0]["status"] == "Started"
    assert any(item["family"] == "fg_a" and item["status"] == "Started" for item in mixed_payload["family_results"])
    assert mixed_payload["completion"]["all_succeeded"] is True
    assert len(fake_sm.calls) == 3

    rc = main(
        [
            "--project-name",
            "ndr-prod",
            "--feature-spec-version",
            "v1",
            "--artifact-family",
            "delta",
            "--range-start-ts-iso",
            "2025-01-01T00:00:00Z",
            "--range-end-ts-iso",
            "2025-01-01T00:15:00Z",
        ]
    )
    assert rc == 0
    no_fgb_payload = json.loads(capsys.readouterr().out.strip())
    assert no_fgb_payload["fg_b_baseline_results"] == []
    assert no_fgb_payload["family_results"][0]["status"] == "Started"
    assert len(fake_sm.calls) == 4
