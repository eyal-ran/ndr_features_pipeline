from ndr.processing.inference_predictions_spec import parse_inference_spec


def test_parse_inference_spec_requires_feature_inputs():
    try:
        parse_inference_spec({"model": {"endpoint_name": "endpoint"}, "output": {"s3_prefix": "s3://out"}})
    except ValueError as exc:
        assert "feature_inputs" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing feature_inputs")


def test_parse_inference_spec_requires_endpoint_name():
    spec = {
        "feature_inputs": {"fg_a": {"s3_prefix": "s3://features"}},
        "output": {"s3_prefix": "s3://out"},
    }
    try:
        parse_inference_spec(spec)
    except ValueError as exc:
        assert "endpoint_name" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing model.endpoint_name")


def test_parse_inference_spec_defaults_dataset_names():
    spec = {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://features"},
            "fg_c": {"s3_prefix": "s3://features"},
        },
        "model": {"endpoint_name": "endpoint"},
        "output": {"s3_prefix": "s3://out"},
        "join_output": {"s3_prefix": "s3://joined"},
    }
    parsed = parse_inference_spec(spec)
    assert parsed.feature_inputs["fg_a"].dataset == "fg_a"
    assert parsed.output.dataset == "inference_predictions"
    assert parsed.join_output.dataset == "prediction_feature_join"


def test_parse_inference_spec_payload_preprocessing_params():
    spec = {
        "feature_inputs": {"fg_a": {"s3_prefix": "s3://features"}},
        "model": {"endpoint_name": "endpoint"},
        "output": {"s3_prefix": "s3://out"},
        "payload": {
            "feature_columns": ["f1", "f2"],
            "scaler_params": {"f1": {"median": 1.0, "iqr": 2.0}},
            "outlier_params": {"f1": {"median": 1.0, "mad": 0.5, "z_max": 6.0}},
        },
    }
    parsed = parse_inference_spec(spec)
    assert parsed.payload.feature_columns == ["f1", "f2"]
    assert parsed.payload.scaler_params["f1"]["iqr"] == 2.0
    assert parsed.payload.outlier_params["f1"]["z_max"] == 6.0
