from ndr.orchestration.backfill_contracts import (
    build_execution_manifest,
    build_family_range_plan,
    build_training_trigger_family_ranges,
)


def test_planner_schema_normalizes_ranges_and_carries_contract_version():
    plan = build_family_range_plan(
        family_ranges={
            "delta": [
                {"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"},
                {"start_ts": "2024-04-01T00:10:00Z", "end_ts": "2024-04-01T00:20:00Z"},
            ],
            "fg_a": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
            "pair_counts": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
            "fg_b_baseline": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T23:59:59Z"}],
            "fg_c": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
        }
    )
    manifest = build_execution_manifest(
        project_name="fw_paloalto",
        feature_spec_version="v1",
        planner_mode="self_detect",
        source="historical_windows_extractor",
        family_plan=plan,
    )

    assert manifest["contract_version"] == "backfill_manifest.v1"
    delta = next(item for item in manifest["family_plan"] if item["family"] == "delta")
    assert delta["ranges"] == [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:20:00Z"}]


def test_selective_decision_contract_respects_requested_families_and_dependencies():
    plan = build_family_range_plan(
        family_ranges={
            "delta": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
            "fg_a": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
            "pair_counts": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
            "fg_b_baseline": [],
            "fg_c": [{"start_ts": "2024-04-01T00:00:00Z", "end_ts": "2024-04-01T00:15:00Z"}],
        },
        requested_families=["fg_c"],
    )
    fg_c = next(item for item in plan if item.family == "fg_c")
    assert fg_c.execute is False
    assert fg_c.reason == "dependency_missing:fg_b_baseline"


def test_training_trigger_contract_maps_missing_windows_for_caller_compatibility():
    family_ranges = build_training_trigger_family_ranges(
        missing_15m_windows=[{"window_start_ts": "2024-04-01T00:00:00Z", "window_end_ts": "2024-04-01T00:15:00Z"}],
        missing_fgb_windows=[{"reference_time_iso": "2024-04-01T00:00:00Z", "horizons": ["7d"]}],
    )
    assert family_ranges["delta"][0]["start_ts"] == "2024-04-01T00:00:00Z"
    assert family_ranges["fg_b_baseline"][0]["end_ts"] == "2024-04-01T23:59:59Z"
