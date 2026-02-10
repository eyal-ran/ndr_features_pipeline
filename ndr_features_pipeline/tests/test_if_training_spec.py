from ndr.processing.if_training_spec import parse_if_training_spec


def test_parse_if_training_spec_requires_fg_inputs():
    spec = {
        "feature_inputs": {"fg_a": {"s3_prefix": "s3://fg-a"}},
        "output": {"artifacts_s3_prefix": "s3://a", "report_s3_prefix": "s3://r"},
        "model": {"version": "v1"},
    }
    try:
        parse_if_training_spec(spec)
    except ValueError as exc:
        assert "feature_inputs.fg_c" in str(exc)
    else:
        raise AssertionError("Expected missing fg_c error")


def test_parse_if_training_spec_happy_path_defaults():
    spec = {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://fg-a"},
            "fg_c": {"s3_prefix": "s3://fg-c"},
        },
        "output": {
            "artifacts_s3_prefix": "s3://models/if_training",
            "report_s3_prefix": "s3://models/reports",
        },
        "model": {"version": "v2"},
    }
    parsed = parse_if_training_spec(spec)
    assert parsed.window.lookback_months == 4
    assert parsed.window.gap_months == 1
    assert parsed.model_version == "v2"
    assert parsed.feature_inputs["fg_a"].dataset == "fg_a"
    assert parsed.reliability.min_rows_per_input == 100
    assert parsed.reliability.min_rows_per_window == 25
    assert parsed.reliability.min_partition_coverage_ratio == 0.8
    assert parsed.reliability.max_rows_per_join_key == 5.0
    assert parsed.deployment.instance_type == "ml.m5.large"
    assert parsed.experiments.enabled is True
    assert parsed.cost_guardrail.option1_monthly_budget_usd == 86.4
    assert parsed.preprocessing.imputation_strategy == "median_scaled"


def test_parse_if_training_spec_uses_validation_gate_improvement():
    spec = {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://fg-a"},
            "fg_c": {"s3_prefix": "s3://fg-c"},
        },
        "output": {
            "artifacts_s3_prefix": "s3://models/if_training",
            "report_s3_prefix": "s3://models/reports",
        },
        "model": {"version": "v2"},
        "tuning": {"min_relative_improvement": 0.01},
        "validation_gates": {"min_relative_improvement": 0.03, "max_score_drift": 0.2},
        "deployment": {"deploy_on_success": True, "endpoint_name": "ndr-if"},
    }
    parsed = parse_if_training_spec(spec)
    assert parsed.validation_gates.min_relative_improvement == 0.03
    assert parsed.validation_gates.max_score_drift == 0.2
    assert parsed.deployment.deploy_on_success is True
    assert parsed.deployment.endpoint_name == "ndr-if"


def test_parse_if_training_cost_guardrail_override():
    spec = {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://fg-a"},
            "fg_c": {"s3_prefix": "s3://fg-c"},
        },
        "output": {
            "artifacts_s3_prefix": "s3://models/if_training",
            "report_s3_prefix": "s3://models/reports",
        },
        "model": {"version": "v2"},
        "cost_guardrail": {"option1_monthly_budget_usd": 50.0, "option4_batch_minutes": 10.0},
    }
    parsed = parse_if_training_spec(spec)
    assert parsed.cost_guardrail.option1_monthly_budget_usd == 50.0
    assert parsed.cost_guardrail.option4_batch_minutes == 10.0


def test_parse_if_training_imputation_override():
    spec = {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://fg-a"},
            "fg_c": {"s3_prefix": "s3://fg-c"},
        },
        "output": {
            "artifacts_s3_prefix": "s3://models/if_training",
            "report_s3_prefix": "s3://models/reports",
        },
        "model": {"version": "v2"},
        "preprocessing": {"imputation_strategy": "constant", "imputation_constant": -1.0},
    }
    parsed = parse_if_training_spec(spec)
    assert parsed.preprocessing.imputation_strategy == "constant"
    assert parsed.preprocessing.imputation_constant == -1.0


def test_parse_if_training_reliability_override():
    spec = {
        "feature_inputs": {
            "fg_a": {"s3_prefix": "s3://fg-a"},
            "fg_c": {"s3_prefix": "s3://fg-c"},
        },
        "output": {
            "artifacts_s3_prefix": "s3://models/if_training",
            "report_s3_prefix": "s3://models/reports",
        },
        "model": {"version": "v2"},
        "reliability": {"min_partition_coverage_ratio": 0.9, "min_rows_per_window": 40, "max_rows_per_join_key": 3.0},
    }
    parsed = parse_if_training_spec(spec)
    assert parsed.reliability.min_partition_coverage_ratio == 0.9
    assert parsed.reliability.min_rows_per_window == 40
    assert parsed.reliability.max_rows_per_join_key == 3.0
