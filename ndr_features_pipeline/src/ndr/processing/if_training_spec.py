"""Typed specification models and parser for IF training jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class TrainingFeatureInputSpec:
    """Defines one required feature input dataset for IF training."""

    s3_prefix: str
    dataset: str | None = None
    required: bool = True


@dataclass
class TrainingWindowSpec:
    """Controls historical lookback and holdout gap windows."""

    lookback_months: int = 4
    gap_months: int = 1


@dataclass
class TrainingPreprocessingSpec:
    """Parameters for deterministic feature preprocessing at train/inference time."""

    eps: float = 1e-6
    scaling_method: str = "robust"
    outlier_method: str = "robust_z_clip"
    z_max: float = 6.0
    imputation_strategy: str = "median_scaled"
    imputation_constant: float = 0.0


@dataclass
class TrainingFeatureSelectionSpec:
    """Configuration for optional feature pruning heuristics."""

    enabled: bool = True
    variance_threshold: float = 1e-12
    corr_threshold: float = 0.95


@dataclass
class TrainingTuningSpec:
    """Hyper-parameter optimization controls used by training orchestration."""

    method: str = "bayesian"
    max_trials: int = 20
    timeout_seconds: int = 3600
    min_relative_improvement: float = 0.0
    search_space: Dict[str, Any] | None = None


@dataclass
class TrainingOutputSpec:
    """Storage locations for model artifacts and training reports."""

    artifacts_s3_prefix: str
    report_s3_prefix: str
    model_image_prefix: str | None = None


@dataclass
class TrainingDeploymentSpec:
    """Settings for optional post-training model deployment."""

    deploy_on_success: bool = False
    endpoint_name: str | None = None
    instance_type: str = "ml.m5.large"
    initial_instance_count: int = 1
    variant_name: str = "AllTraffic"
    strategy: str | None = None
    rollback_on_alarm: bool = True


@dataclass
class TrainingValidationGatesSpec:
    """Acceptance thresholds evaluated before deployment."""

    min_relative_improvement: float = 0.0
    max_alert_volume_delta: float = 0.1
    max_score_drift: float = 0.5


@dataclass
class TrainingReliabilitySpec:
    """Minimum data-quality and reliability guardrails for training."""

    min_rows_per_input: int = 100
    min_rows_per_window: int = 25
    min_join_rows: int = 100
    min_join_coverage_ratio: float = 0.5
    min_partition_coverage_ratio: float = 0.8
    max_rows_per_join_key: float = 5.0
    max_retries: int = 3
    backoff_seconds: float = 1.0




@dataclass
class TrainingCostGuardrailSpec:
    """Budget constraints and estimated spend assumptions for training."""

    enabled: bool = True
    option1_hourly_rate_usd: float = 0.12
    option1_monthly_budget_usd: float = 86.4
    option4_batch_minutes: float = 8.0
    option4_runs_per_month: int = 2880

@dataclass
class TrainingExperimentsSpec:
    """Experiment tracking controls for metadata lineage."""

    enabled: bool = True
    experiment_name: str | None = None
    trial_prefix: str = "if-training"


@dataclass
class IFTrainingSpec:
    """Fully parsed IF training specification used by runner code."""

    feature_inputs: Dict[str, TrainingFeatureInputSpec]
    output: TrainingOutputSpec
    model_version: str
    join_keys: List[str]
    window: TrainingWindowSpec
    preprocessing: TrainingPreprocessingSpec
    feature_selection: TrainingFeatureSelectionSpec
    tuning: TrainingTuningSpec
    deployment: TrainingDeploymentSpec
    validation_gates: TrainingValidationGatesSpec
    reliability: TrainingReliabilitySpec
    experiments: TrainingExperimentsSpec
    cost_guardrail: TrainingCostGuardrailSpec
    random_seed: int = 42


@dataclass
class IFTrainingRuntimeConfig:
    """Runtime context injected by orchestration for a single run."""

    project_name: str
    feature_spec_version: str
    run_id: str
    execution_ts_iso: str
    training_start_ts: str | None = None
    training_end_ts: str | None = None
    eval_start_ts: str | None = None
    eval_end_ts: str | None = None
    missing_windows_override: str = "[]"
    stage: str = "train"


def parse_if_training_spec(job_spec: Dict[str, Any]) -> IFTrainingSpec:
    """Validate and normalize the IF training section of a JobSpec payload."""

    feature_inputs_payload = job_spec.get("feature_inputs")
    if not feature_inputs_payload:
        raise ValueError("IF training JobSpec must include feature_inputs")

    inputs: Dict[str, TrainingFeatureInputSpec] = {}
    for name in ("fg_a", "fg_c"):
        payload = feature_inputs_payload.get(name)
        if not payload:
            raise ValueError(f"feature_inputs.{name} is required")
        if "s3_prefix" not in payload:
            raise ValueError(f"feature_inputs.{name} requires s3_prefix")
        inputs[name] = TrainingFeatureInputSpec(
            s3_prefix=payload["s3_prefix"],
            dataset=payload.get("dataset") or name,
            required=payload.get("required", True),
        )

    output_payload = job_spec.get("output") or {}
    if "artifacts_s3_prefix" not in output_payload:
        raise ValueError("IF training output.artifacts_s3_prefix is required")
    if "report_s3_prefix" not in output_payload:
        raise ValueError("IF training output.report_s3_prefix is required")

    model_payload = job_spec.get("model") or {}
    model_version = model_payload.get("version")
    if not model_version:
        raise ValueError("IF training model.version is required")

    window_payload = job_spec.get("window") or {}
    window = TrainingWindowSpec(
        lookback_months=int(window_payload.get("lookback_months", 4)),
        gap_months=int(window_payload.get("gap_months", 1)),
    )

    tuning_payload = job_spec.get("tuning") or {}
    gates_payload = job_spec.get("validation_gates") or {}
    min_rel_improvement = float(
        gates_payload.get(
            "min_relative_improvement",
            tuning_payload.get("min_relative_improvement", 0.0),
        )
    )

    return IFTrainingSpec(
        feature_inputs=inputs,
        output=TrainingOutputSpec(
            artifacts_s3_prefix=output_payload["artifacts_s3_prefix"],
            report_s3_prefix=output_payload["report_s3_prefix"],
            model_image_prefix=output_payload.get("model_image_prefix"),
        ),
        model_version=model_version,
        join_keys=job_spec.get("join_keys", ["host_ip", "window_label", "window_end_ts"]),
        window=window,
        preprocessing=TrainingPreprocessingSpec(**(job_spec.get("preprocessing") or {})),
        feature_selection=TrainingFeatureSelectionSpec(**(job_spec.get("feature_selection") or {})),
        tuning=TrainingTuningSpec(**tuning_payload),
        deployment=TrainingDeploymentSpec(**(job_spec.get("deployment") or {})),
        validation_gates=TrainingValidationGatesSpec(
            min_relative_improvement=min_rel_improvement,
            max_alert_volume_delta=float(gates_payload.get("max_alert_volume_delta", 0.1)),
            max_score_drift=float(gates_payload.get("max_score_drift", 0.5)),
        ),
        reliability=TrainingReliabilitySpec(**(job_spec.get("reliability") or {})),
        experiments=TrainingExperimentsSpec(**(job_spec.get("experiments") or {})),
        cost_guardrail=TrainingCostGuardrailSpec(**(job_spec.get("cost_guardrail") or {})),
        random_seed=int(job_spec.get("random_seed", 42)),
    )
