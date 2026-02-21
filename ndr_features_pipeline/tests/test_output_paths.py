from ndr.processing.output_paths import build_batch_output_prefix, build_fg_b_publication_metadata


def test_build_batch_output_prefix_has_dataset_suffix_and_timestamp_path():
    out = build_batch_output_prefix(
        base_prefix="s3://bucket/projects/p1/features",
        dataset="fg_b",
        batch_start_ts_iso="2025-01-31T12:34:56Z",
        batch_id="batch-1",
    )
    assert out == "s3://bucket/projects/p1/features/fg_b/ts=2025/01/31/12/34-batch_id=batch-1"


def test_build_fg_b_publication_metadata_uses_reference_time_as_created_fields():
    metadata = build_fg_b_publication_metadata(
        feature_spec_version="v1",
        baseline_horizon="30d",
        reference_time_iso="2025-12-31T00:00:00+00:00",
        run_mode="REGULAR",
        run_batch_id="monthly-42",
        baseline_start_ts="2025-11-24T00:00:00Z",
        baseline_end_ts="2025-12-24T00:00:00Z",
    )

    assert metadata["feature_spec_version"] == "v1"
    assert metadata["baseline_horizon"] == "30d"
    assert metadata["reference_time_iso"] == "2025-12-31T00:00:00Z"
    assert metadata["created_at"] == "2025-12-31T00:00:00Z"
    assert metadata["created_date"] == "2025-12-31"
    assert metadata["baseline_start_ts"] == "2025-11-24T00:00:00Z"
    assert metadata["baseline_end_ts"] == "2025-12-24T00:00:00Z"
