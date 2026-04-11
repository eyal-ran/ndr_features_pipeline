from ndr.config.batch_index_prefixes import BatchPrefixPrecomputeRequest, precompute_batch_index_prefixes


def _dpp_code_metadata():
    return {
        "delta_step": {"code_artifact_s3_uri": "s3://code/dpp/delta.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a1", "artifact_format": "tar.gz"},
        "fg_a_step": {"code_artifact_s3_uri": "s3://code/dpp/fga.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a2", "artifact_format": "tar.gz"},
        "pair_counts_step": {"code_artifact_s3_uri": "s3://code/dpp/pc.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a3", "artifact_format": "tar.gz"},
        "fg_c_step": {"code_artifact_s3_uri": "s3://code/dpp/fgc.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a4", "artifact_format": "tar.gz"},
        "fg_b_step": {"code_artifact_s3_uri": "s3://code/dpp/fgb.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a5", "artifact_format": "tar.gz"},
        "machine_inventory_unload_step": {"code_artifact_s3_uri": "s3://code/dpp/miu.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a6", "artifact_format": "tar.gz"},
        "backfill_extractor_step": {"code_artifact_s3_uri": "s3://code/dpp/bfe.tar.gz", "artifact_build_id": "b1", "artifact_sha256": "a7", "artifact_format": "tar.gz"},
    }


def _mlp_code_metadata():
    return {
        "inference_step": {"code_artifact_s3_uri": "s3://code/mlp/inf.tar.gz", "artifact_build_id": "b2", "artifact_sha256": "m1", "artifact_format": "tar.gz"},
        "join_step": {"code_artifact_s3_uri": "s3://code/mlp/join.tar.gz", "artifact_build_id": "b2", "artifact_sha256": "m2", "artifact_format": "tar.gz"},
        "training_step": {"code_artifact_s3_uri": "s3://code/mlp/train.tar.gz", "artifact_build_id": "b2", "artifact_sha256": "m3", "artifact_format": "tar.gz"},
    }


def test_precompute_returns_full_dpp_and_mlp_contract_shape():
    result = precompute_batch_index_prefixes(
        BatchPrefixPrecomputeRequest(
            project_name="fw_paloalto",
            batch_id="mb-1",
            raw_parsed_logs_s3_prefix="s3://raw/fw_paloalto/org1/org2/2025/01/01/mb-1/",
            etl_ts="2025-01-01T10:20:00Z",
            ml_project_names=["ml-a", "ml-b"],
            dpp_roots={
                "delta_root": "s3://ml/dpp",
                "pair_counts_root": "s3://ml/dpp",
                "fg_a_root": "s3://ml/dpp",
                "fg_b_root": "s3://ml/dpp",
                "fg_c_root": "s3://ml/dpp",
                "machine_inventory_root": "s3://ml/dpp",
            },
            mlp_roots_by_project={
                "ml-a": {
                    "predictions_root": "s3://ml/mlp/ml-a",
                    "prediction_join_root": "s3://ml/mlp/ml-a",
                    "publication_root": "s3://ml/mlp/ml-a",
                    "training_reports_root": "s3://ml/mlp/ml-a/training/events",
                    "training_artifacts_root": "s3://ml/mlp/ml-a/training/events",
                    "production_model_root": "s3://ml/mlp/ml-a/production/model",
                },
                "ml-b": {
                    "predictions_root": "s3://ml/mlp/ml-b",
                    "prediction_join_root": "s3://ml/mlp/ml-b",
                    "publication_root": "s3://ml/mlp/ml-b",
                    "training_reports_root": "s3://ml/mlp/ml-b/training/events",
                    "training_artifacts_root": "s3://ml/mlp/ml-b/training/events",
                    "production_model_root": "s3://ml/mlp/ml-b/production/model",
                },
            },
            dpp_code_paths={k: f"s3://code/dpp/{k}.py" for k in _dpp_code_metadata()},
            mlp_code_paths_by_project={
                "ml-a": {k: f"s3://code/mlp/ml-a/{k}.py" for k in _mlp_code_metadata()},
                "ml-b": {k: f"s3://code/mlp/ml-b/{k}.py" for k in _mlp_code_metadata()},
            },
            dpp_code_metadata=_dpp_code_metadata(),
            mlp_code_metadata_by_project={"ml-a": _mlp_code_metadata(), "ml-b": _mlp_code_metadata()},
        )
    )

    assert result.date_partition == "2025/01/01"
    assert result.hour == "10"
    assert result.within_hour_run_number == "2"
    assert result.s3_prefixes["dpp"]["fg_b"]["machines_manifest"].endswith("/fg_b/machines_manifest/manifest.json")
    assert set(result.s3_prefixes["mlp"].keys()) == {"ml-a", "ml-b"}
    assert result.s3_prefixes["mlp"]["ml-a"]["prediction_join"].endswith("/prediction_join/part-00000.parquet")


def test_precompute_rejects_missing_code_metadata():
    req = BatchPrefixPrecomputeRequest(
        project_name="fw_paloalto",
        batch_id="mb-1",
        raw_parsed_logs_s3_prefix="s3://raw/fw_paloalto/org1/org2/2025/01/01/mb-1/",
        etl_ts="2025-01-01T10:20:00Z",
        ml_project_names=["ml-a"],
        dpp_roots={
            "delta_root": "s3://ml/dpp",
            "pair_counts_root": "s3://ml/dpp",
            "fg_a_root": "s3://ml/dpp",
            "fg_b_root": "s3://ml/dpp",
            "fg_c_root": "s3://ml/dpp",
            "machine_inventory_root": "s3://ml/dpp",
        },
        mlp_roots_by_project={
            "ml-a": {
                "predictions_root": "s3://ml/mlp/ml-a",
                "prediction_join_root": "s3://ml/mlp/ml-a",
                "publication_root": "s3://ml/mlp/ml-a",
                "training_reports_root": "s3://ml/mlp/ml-a/training/events",
                "training_artifacts_root": "s3://ml/mlp/ml-a/training/events",
                "production_model_root": "s3://ml/mlp/ml-a/production/model",
            }
        },
        dpp_code_paths={k: f"s3://code/dpp/{k}.py" for k in _dpp_code_metadata()},
        mlp_code_paths_by_project={"ml-a": {k: f"s3://code/mlp/ml-a/{k}.py" for k in _mlp_code_metadata()}},
        dpp_code_metadata=_dpp_code_metadata(),
        mlp_code_metadata_by_project={"ml-a": {"inference_step": {"code_artifact_s3_uri": "s3://x"}}},
    )

    try:
        precompute_batch_index_prefixes(req)
    except KeyError as exc:
        assert "artifact_build_id" in str(exc)
    else:
        raise AssertionError("expected missing metadata validation error")
