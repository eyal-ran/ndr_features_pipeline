# NDR Isolation Forest Training & Inference Protocol (FG-A + FG-C)

## Purpose
This document defines the **implementation plan** and **training protocol** for the NDR
Isolation Forest model using **FG-A (current behavior)** and **FG-C (correlation features)**.
It also specifies the preprocessing steps and selection logic that must be **replicated at
inference** to avoid training/serving skew.

FG-C is derived from FG-A and FG-B baselines. Training on FG-A + FG-C therefore captures
current behavior plus correlations to baselines (including pair-count baselines indirectly).

## Scope
- Training dataset: FG-A + FG-C joined on host/time keys.
- Windowing: **3 months of data** with a **1‑month gap** (exclude most recent month).
- Model: Isolation Forest with hyperparameter optimization.
- Feature selection: explicit, reproducible, and persisted for inference.
- Preprocessing: robust scaling, outlier handling, and metadata separation.
- Inference: **parallel pipeline** that mirrors training preprocessing and feature mask.
- Training outputs: metrics tracked in SageMaker Experiments and artifacts written to S3.

## Data Inputs & Feature Sources
### Feature Groups
- **FG-A**: current behavior features and metadata fields.
- **FG-C**: correlation features between FG-A and FG-B baselines (z-scores, ratios,
  magnifiers, drift metrics).

### Join Keys (recommended)
- `host_ip`, `window_label`, `window_end_ts`

### Training Window Definition
Let `T0` be the execution time (UTC).
- **Excluded window (gap)**: `[T0 - 1 month, T0)`
- **Training window**: `[T0 - 4 months, T0 - 1 month)`
- This yields **3 months of history** while avoiding the most recent month to prevent
  immediate feedback loops and reduce leakage from recent incidents.

## Training Pipeline Architecture (SageMaker)
Training should be implemented as a **dedicated SageMaker Pipeline** (separate from the
15‑minute inference pipeline). The pipeline can be:
- Triggered manually for an explicit version refit.
- Triggered automatically by Step Functions in response to drift detection, using
  project parameters resolved from DynamoDB.

### Recommended Pipeline Steps
1. **Resolve Training Parameters**
   - Load project/job configuration from DynamoDB (version, input prefixes, training window).
2. **Read Features from S3**
   - Load FG-A + FG-C partitions for `[T0 - 4mo, T0 - 1mo)`.
3. **Schema Enforcement**
   - Enforce FG-A and FG-C schemas independently and fail fast on missing columns.
4. **Feature Join**
   - Join FG-A + FG-C on host/time keys.
5. **Preprocessing**
   - Separate metadata vs numeric features.
   - Apply robust scaling and outlier clipping.
   - Apply imputation if defaults are not configured.
6. **Feature Selection**
   - Run near-constant filter, correlation pruning, stability selection, permutation proxy.
7. **Hyperparameter Tuning**
   - Run Bayesian optimization with time-blocked validation.
8. **Train Final Model**
   - Fit Isolation Forest with best parameters on the full training window.
9. **Persist Artifacts + Metrics**
   - Save model, scalers, outlier params, feature mask, metrics, and reproducibility report.
10. **Write Final Model Image Copy**
    - Copy the final best model image/package (e.g., `model.tar.gz`) to a dedicated S3 location
      for deployment and audit traceability.
11. **Register Outputs**
    - Write results to S3 and log metrics in SageMaker Experiments.
12. **Conditional Deployment**
    - If `deploy_on_success=true` (from DynamoDB/parameter input), execute endpoint upsert:
      - if endpoint exists: replace currently deployed model with selected model version
      - if endpoint does not exist: create endpoint using provided endpoint parameters,
        then deploy selected model

## Reliability & Correctness Guardrails (Implementation Critical)
To make implementation accurate, efficient, and low-risk in production, enforce the
following execution guardrails in the training pipeline:

### A) Preflight Input Validation (fail-fast)
Before expensive processing/tuning starts, validate:
- required S3 prefixes exist and are readable
- resolved date partitions for the full training window are present (or satisfy
  minimum data coverage thresholds)
- minimum row counts per horizon/window are met
- FG-A/FG-C join-key cardinality sanity checks (unexpected key explosion/duplication)

If any hard guardrail fails, stop the run and write failure context to the trial component
and reproducibility report.

### B) Determinism & Reproducibility
- Fix random seeds for sampling, feature selection bootstrap, and model training.
- Persist library/runtime versions in the final report.
- Persist exact resolved parameter payload from DynamoDB in the final report.

### C) Idempotent Artifact Writes
- Write all outputs under a run-scoped prefix keyed by run ID.
- Use a `SUCCESS`/manifest marker file only after all required artifacts are present.
- Never overwrite “last-known-good” pointers until validation gates pass.

### D) Promotion Gates (model acceptance)
Promote/deploy model only if all are true:
- schema + preprocessing parity checks pass
- validation objective improves beyond configured threshold (`min_relative_improvement`)
- alert volume guardrail is within approved bounds
- drift/stability metrics are within configured thresholds

### E) Safe Rollback/Recovery
- Keep previous model artifact + endpoint config as rollback target.
- On deployment failure or post-deploy alarm, auto-rollback and record status.
- Keep retries bounded with exponential backoff for transient AWS failures.

### F) Performance Controls (efficiency)
- Cache/reuse intermediate datasets where beneficial.
- Prune non-required columns early to reduce I/O and memory pressure.
- Bound tuning trials/time (`max_trials`, `timeout_seconds`) from DynamoDB config.

## Schema Enforcement (Training + Inference)
All input DataFrames MUST be schema-enforced prior to modeling to ensure:
- consistent column presence
- correct data types
- default values where configured

### Implementation Guidance
- Apply schema enforcement to FG-A and FG-C outputs independently, then join.
- If a required column is missing, fail fast.
- **If a feature does not have a schema default**, it can still contain nulls —
  plan for explicit imputation when defaults are absent.

## Feature Set Assembly (FG-A + FG-C)
### Metadata vs Model Features
- **Metadata fields** (e.g., host/time context) must be **excluded from model inputs**
  but retained for joins, diagnostics, and output attribution.
- **Model features** include:
  - FG-A numeric metrics
  - FG-C numeric correlation features

### Feature Mask (Persisted for Inference)
If feature selection yields a subset that performs better than the full set:
- Persist the **feature mask** (ordered list of selected columns)
  to the DynamoDB project parameters table.
- At inference, use this feature mask to filter the payload
  (see `inference_predictions.spec.payload.feature_columns`).

## Preprocessing (Training + Inference)
All preprocessing steps below MUST be applied **identically** during inference.

### 1) Robust Scaling (required)
Use a robust scaler based on median and IQR:

```
scaled_x = (x - median(x)) / (IQR(x) + eps)
IQR(x) = P75(x) - P25(x)
```

Implementation details:
- Compute medians and IQRs on the **training window only**.
- Persist these statistics for inference.
- Use `eps` (e.g., `1e-6`) to avoid division by zero.

### 2) Missing Values / Imputation
Schema enforcement only fills defaults if `default_value` is defined.
Therefore:
- If defaults cover all feature columns: **no additional imputation is required**.
- If not, apply a consistent imputation rule (e.g., fill with median after scaling).

### 3) Outlier Handling (domain-agnostic)
Apply robust z-score clipping to reduce the impact of extreme tails:

```
robust_z = (x - median(x)) / (MAD(x) + eps)
clipped_z = clip(robust_z, -Z_MAX, +Z_MAX)
```

Recommended:
- `Z_MAX = 6` (consistent with FG-C robust z-score clipping patterns).
- Persist `Z_MAX` and statistics for inference.

## Feature Selection (Training)
Because FG-C produces a large number of correlated features, feature selection is
recommended to improve generalization and reduce alert noise.

### Step A: Near-Constant Filter
- Drop features with variance below a minimum threshold.

### Step B: Correlation Pruning
- Compute pairwise correlations (Spearman or Pearson).
- For any pair with |ρ| > 0.95, drop the feature with lower stability/importance.

### Step C: Stability Selection (Unsupervised)
- Train multiple Isolation Forests on bootstrap samples.
- Track how frequently each feature influences score variance.
- Keep features with high stability across runs.

### Step D: Permutation Importance (Unsupervised Proxy)
- On the validation month, permute each feature and measure:
  - score distribution shift
  - rank ordering stability
- Drop features with minimal impact.

### Final Feature Mask
- Compare full vs reduced feature set.
- If reduced set performs better, persist the **feature mask** in DynamoDB for inference.

## Hyperparameter Tuning (Isolation Forest)
### Recommended Method: Bayesian Optimization (Optuna)
Bayesian optimization provides sample-efficient tuning for high-dimensional inputs.

### Search Space (example)
- `n_estimators`: 200–800
- `max_samples`: 0.2–1.0
- `max_features`: 0.3–1.0
- `contamination`: 0.001–0.05
- `bootstrap`: [True, False]

### Validation Strategy
Use a **time-blocked holdout** from the training window:
- Train: months ‑4 to ‑2
- Validate: month ‑1

### Optimization Objective (composite)
Minimize a weighted objective that balances:
- alert volume stability
- score distribution drift
- score rank stability
- (optional) downstream precision@K if labels exist

## Inference Pipeline (Parallel to Training)
To avoid skew, inference must mirror training exactly:

1. Schema enforcement on FG-A and FG-C.
2. Join to form the same feature frame.
3. Apply **the same feature mask** from DynamoDB.
4. Apply **the same scaling and clipping** parameters.
5. Run model inference.

Any mismatch in preprocessing or feature mask invalidates the model.


## Conditional Deployment (Training Pipeline)
Deployment is executed only when a deployment flag is enabled in DynamoDB training
parameters (e.g., `deploy_on_success=true`).

Recommended deployment behavior:
- Register new model package/version with metadata tags (project/version/run ID).
- Update endpoint only after model validation gates pass.
- Perform endpoint **upsert** semantics:
  - If endpoint exists, create/update endpoint config to point to the newly selected model,
    then update endpoint in-place (blue/green or canary preferred).
  - If endpoint does not exist, create model + endpoint config + endpoint using provided
    deployment parameters.
- Use blue/green or canary endpoint update policy where possible.
- Write deployment outcome (success/failure, endpoint config, rollback status) to the
  final report and to SageMaker Experiments.

Required deployment parameters (from DynamoDB training spec):
- `deployment.deploy_on_success`
- `deployment.endpoint_name`
- `deployment.instance_type` (or serverless config)
- `deployment.initial_instance_count`
- `deployment.variant_name`
- Optional safety/rollout controls: `deployment.strategy`, `deployment.max_traffic_shift_pct`,
  `deployment.bake_time_minutes`, `deployment.rollback_on_alarm`

## Cost Guardrail & Option-4 Fallback Trigger
Default deployment choice is **Option 1** (always-on right-sized endpoint).

Estimated monthly compute-only costs for reference (single endpoint/job compute at $0.12/hour):
- **Option 1 (always-on endpoint)**: `720h * $0.12 = $86.40/month`
- **Option 4 (batch-style every 15m, 2,880 runs/month)**:
  - ~4 min/run: `192h * $0.12 = $23.04/month`
  - ~8 min/run: `384h * $0.12 = $46.08/month`
  - ~12 min/run: `576h * $0.12 = $69.12/month`

Guardrail recommendation:
- If observed Option-1 monthly total cost (including endpoint, data processing overhead,
  and operational overhead) consistently exceeds estimated/approved budget thresholds,
  initiate an architecture review for **Option 4**.

Option-4 details to evaluate during review:
- Run batch inference every 15 minutes via SageMaker Batch Transform or Processing job.
- Compare end-to-end latency/SLA impact against endpoint mode.
- Include startup overhead, orchestration complexity, and retry behavior in TCO analysis.
- Adopt Option 4 only if cost savings remain material after including operational costs
  and SLA risk.

## Version 2.0 Enhancements (Roadmap)
### Ensemble Strategies
- Isolation Forest + LOF + Autoencoder
- Weighted score fusion or rank aggregation
- Rule-based overrides for high-risk patterns

### False Positive Reduction
- Contextual suppression (maintenance windows)
- Cold-start routing for new hosts
- Alert deduplication and aggregation
- Feedback-driven allowlists
- Risk-tiered routing to different queues

## Implementation Checklist
1. Implement training window selection `[T0 - 4mo, T0 - 1mo)`.
2. Enforce schema for FG-A and FG-C.
3. Join FG-A + FG-C on host/time keys.
4. Separate metadata vs numeric model features.
5. Apply robust scaling and outlier clipping.
6. Run feature selection and persist feature mask if improved.
7. Optimize Isolation Forest parameters via Bayesian optimization.
8. Save model, scaler stats, clipping settings, feature mask, and model image copy.
9. Emit a full reproducibility report (inputs, preprocessing, tuning, outputs) and write it to S3.
10. Mirror all preprocessing in inference.
11. Record training outputs in SageMaker Experiments and S3.
12. Deploy model to endpoint when deployment flag is enabled.
13. If Option-1 actual monthly costs exceed guardrail thresholds, evaluate Option 4
    (batch-style inference) with measured latency/cost trade-off before switching.
14. Enforce preflight data validation and fail-fast checks before tuning/training.
15. Apply promotion gates and rollback policy for safe model replacement.
16. Use run-scoped idempotent output writes and success markers before promotion.

## Training Outputs & Experiment Tracking
### SageMaker Experiments
Each training run should create or update:
- **Experiment**: project-level grouping for model training.
- **Trial**: one per training run (includes version and time window).
- **Trial Components**: preprocessing stats, tuning results, final model metrics.

Log the following metrics at minimum:
- Training window start/end timestamps
- Feature count (pre/post selection)
- Validation objective (composite)
- Alert volume and score stability metrics
- Final model parameters

### S3 Artifacts (Partitioned)
Persist artifacts under an S3 prefix partitioned by **model version** and **training time**:

```
s3://<models-bucket>/if_training/
  model_version=<version>/
    training_start=<YYYY-MM-DD>/
      training_end=<YYYY-MM-DD>/
        model/
        preprocessing/
          scaler_params.json
          outlier_params.json
          feature_mask.json
        metrics/
          tuning_metrics.json
          final_metrics.json
```

These artifacts must be referenced during inference to guarantee
preprocessing parity.


### Final Reproducibility Report (required)
Emit a machine-readable final report for each successful run and store it in S3
(alongside model artifacts).

Recommended file name:
- `reports/final_training_report.json`

Required report fields:
- run metadata: project, feature spec version, model version, training run ID
- exact input datasets: FG-A/FG-C S3 prefixes, resolved partitions, row counts
- effective training window: start/end, excluded month gap
- schema manifests and join keys used
- preprocessing applied: scaling/imputation/outlier parameters and sequence
- feature selection process: filters run, thresholds, selected feature mask
- tuning summary: search space, trials, best hyperparameters, objective values
- final model details: artifact paths, model image copy path, hash/checksum
- validation/final metrics and thresholding settings
- deployment decision: deploy flag, endpoint name, deployment status
- validation gates status: each gate pass/fail with threshold/value
- deterministic context: random seeds, package/runtime versions, run-scoped output prefixes
- data-quality summary: input partition availability, row counts, join cardinality checks

This report is the canonical reproducibility artifact and must be linkable from
SageMaker Experiments Trial Components.


## DynamoDB Configuration Strategy (Training-Specific)
To avoid overloading unrelated job specs, introduce a dedicated training record:
- `job_name = "if_training"` (versioned as `if_training#<feature_spec_version>`).

This record should be the source of truth for training runtime controls:
- input S3 prefixes (`fg_a_input`, `fg_c_input`)
- output S3 prefixes (`artifacts_output`, `report_output`, `experiments_output`)
- time window policy (`lookback_months=4`, `gap_months=1`)
- preprocessing config (`eps`, scaling method, outlier clipping settings)
- feature selection toggles and thresholds
- tuning config (search space, objective weights, max trials, timeout)
- deployment config (`deploy_on_success`, `endpoint_name`, `deployment_strategy`)
- validation gates (`min_relative_improvement`, `max_alert_volume_delta`, `max_score_drift`)
- reliability controls (`retry_policy`, `max_retries`, `backoff_seconds`, `run_id`)

Inference should continue reading its own `inference_predictions` record, while
training writes back selected outputs (feature mask, scaler/outlier params, model version)
for inference consumption.

## DynamoDB Parameters (Required Additions)
Recommended fields under `inference_predictions.spec`:
- `payload.feature_columns` → selected feature mask
- `payload.scaler_params` → medians + IQRs per feature
- `payload.outlier_params` → MADs + Z_MAX

These values ensure inference replicates training preprocessing.
