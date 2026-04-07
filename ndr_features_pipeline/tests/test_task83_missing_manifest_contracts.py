from ndr.orchestration.training_missing_manifest import CONTRACT_VERSION, ensure_manifest, from_missing_sources


def test_missing_manifest_schema_is_canonical_and_versioned():
    manifest = from_missing_sources(
        missing_15m_windows=[
            {"window_start_ts": "2024-04-01T00:00:00Z", "window_end_ts": "2024-04-01T00:15:00Z"},
            {"window_start_ts": "2024-04-01T00:10:00Z", "window_end_ts": "2024-04-01T00:30:00Z"},
        ],
        missing_fgb_windows=[{"reference_time_iso": "2024-04-02T00:00:00Z", "horizons": ["7d"]}],
        project_name="proj",
        feature_spec_version="v1",
        ml_project_name="mlp",
        run_id="run-1",
    )
    assert manifest["contract_version"] == CONTRACT_VERSION
    assert [entry["artifact_family"] for entry in manifest["entries"]] == ["fg_a_15m", "fg_b_daily"]
    fg_a_ranges = manifest["entries"][0]["ranges"]
    assert fg_a_ranges == [{"start_ts_iso": "2024-04-01T00:00:00Z", "end_ts_iso": "2024-04-01T00:30:00Z"}]


def test_missing_manifest_rejects_unknown_version():
    payload = {"contract_version": "if_training_missing_windows.v0", "entries": []}
    try:
        ensure_manifest(payload)
    except ValueError as exc:
        assert "Unsupported missing-window manifest version" in str(exc)
    else:
        raise AssertionError("Expected schema-version guard failure")
