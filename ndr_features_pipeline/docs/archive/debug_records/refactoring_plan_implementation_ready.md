# Refactor Plan

## 1. General refactoring purpose

### 1.1 What this refactor is trying to achieve
The refactor exists to make the entire NDR orchestration and processing stack:

- deterministic,
- DDB-driven,
- prefix-schema-driven,
- batch-index-driven,
- generic across cadences,
- ML-project-aware,
- contract-validated, and
- safe to operate under backfill / retraining / multi-ML-project fan-out.

Today, the code is not consistently doing that.

### 1.2 Current misalignments with the desired end state
The most important current misalignments found by code inspection are:

#### A. RT orchestration does not truly fan out per `ml_project_name`
The RT father Step Function accepts `ml_project_name` / `ml_project_names`, but does not create a Map branch over them before running DPP + inference + publication. Instead, it forwards ML-project fields into pipelines.
Intended fix: normalize to array-first `ml_project_names`, validate before reads, then execute strict per-branch fan-out with explicit branch context.
Planned implementation tasks: Task 0, Task 3, Task 4, Task 5.

#### B. Several SF → SageMaker Pipeline interfaces are malformed
There are cases where Step Functions pass pipeline parameters that the target pipeline does not define, and cases where a pipeline is started without parameters its steps require.
Intended fix: freeze interface matrix (SF input -> pipeline params -> CLI args -> runtime fields) and enforce it with contract tests.
Planned implementation tasks: Task 0, Task 2, Task 4, Task 9.

#### C. Pipeline code-location resolution is performed at pipeline-build time using placeholder defaults
All pipeline builders call `resolve_step_code_uri()`, which loads JobSpec immediately using builder-time values like `"<required:ProjectName>"`.
Intended fix: resolve step code using concrete DDB contract identities only; add explicit guard tests for placeholder-based resolution.
Planned implementation tasks: Task 2, Task 8, Task 9.

#### D. `ml_project_name` is frequently declared but not actually consumed by jobs
The inference pipeline, prediction-join pipeline, and IF training pipeline define `MlProjectName`, but job arguments do not propagate it to CLI entrypoints or runtime configs.
Intended fix: enforce end-to-end branch-context propagation in every flow segment that consumes MLP paths or branch-scoped artifacts.
Planned implementation tasks: Task 0, Task 2, Task 5, Task 8, Task 9.

#### E. Delta → FG-A is not contract-safe enough
FG-A expects `mini_batch_id` in Delta output, but in the inspected Delta build path the main Delta dataframe is not explicitly given `mini_batch_id` before write.
Intended fix: lock Delta→FG-A mini-batch contract and enforce via deterministic write/read checks and fail-fast behavior.
Planned implementation tasks: Task 5, Task 9.

#### F. Backfill extractor is not properly wired into the backfill map
The extractor writes a manifest to S3, but the Step Function map reads windows only from input payload fields rather than reading the manifest result.
Intended fix: backfill map must consume extractor manifest output directly and execute selective family/range recovery deterministically.
Planned implementation tasks: Task 7, Task 8, Task 9.

#### G. Machine inventory still has a query fallback
The machine inventory code does not follow the desired “UNLOAD only” policy yet.
Intended fix: remove fallback and adopt UNLOAD-only policy with flow-specific DPP query contract and deterministic staging.
Planned implementation tasks: Task 6, Task 7, Task 9.

#### H. Training remediation sequencing is structurally misordered
Training remediation depends on planning outputs that are currently produced too late (in train stage), which breaks “backfill-if-needed-before-train” intent.
Intended fix: enforce `verify -> plan -> remediate -> reverify -> train` stage contract with hard train gate.
Planned implementation tasks: Task 8, Task 9.

#### I. Training missing-window handling is inconsistent and non-selective
Current training flow mixes missing-partition and missing-window semantics, may collapse sparse gaps into coarse min/max envelopes, and may truncate remediation scope by retry slicing.
Intended fix: enforce one normalized missing-manifest schema and selective remediation execution contract.
Planned implementation tasks: Task 7, Task 8, Task 9.

#### J. Required runtime fields are present but not operationally used
Fields such as `batch_index_table_name` / `mlp_config_table_name` are required in training runtime but not used as authoritative decision inputs.
Intended fix: wire required fields into actual planner/resolution logic or remove them from required runtime contract.
Planned implementation tasks: Task 2, Task 8, Task 9.

#### K. Orchestration target resolution still allows non-canonical business fallback behavior
Code-default/env fallback behavior undermines strict DDB-driven orchestration target ownership and deterministic behavior.
Intended fix: DDB contract becomes required source for enabled branches; invalid/missing targets fail fast.
Planned implementation tasks: Task 0, Task 8, Task 9.

#### L. Runtime payload surface is too large and duplicates DDB-owned configuration
Too many runtime toggles/overrides are passed through SF/pipeline layers instead of being loaded from DDB contracts.
Intended fix: minimize trigger payload to run-varying fields only; load all business policy from DDB.
Planned implementation tasks: Task 0, Task 2, Task 8, Task 9.

These are the 1A–1L malfunctions to make central in the fix plan.

### 1.2.1 Known-issue coverage matrix (must stay complete)
Every issue must have both (a) an explicit intended fix and (b) at least one concrete implementation task.

| Issue ID | Short title | Intended fix anchor | Implemented in task(s) |
|---|---|---|---|
| 1A | RT fan-out not real | Array-first branch fan-out + validation-before-read + explicit branch context | 0, 3, 4, 5 |
| 1B | SF↔Pipeline interface mismatches | Locked interface matrix + contract tests | 0, 2, 4, 9 |
| 1C | Placeholder-based code URI resolution | Concrete DDB code-contract resolution only | 2, 8, 9 |
| 1D | `ml_project_name` propagation gaps | End-to-end branch context propagation contract | 0, 2, 5, 8, 9 |
| 1E | Delta→FG-A mini-batch contract gap | Deterministic mini-batch field contract + checks | 5, 9 |
| 1F | Backfill extractor/map disconnect | Manifest-driven map fanout + selective execution | 7, 8, 9 |
| 1G | Machine inventory fallback behavior | UNLOAD-only inventory path | 6, 7, 9 |
| 1H | Training remediation sequencing | Enforced verify/plan/remediate/reverify/train lifecycle | 8, 9 |
| 1I | Non-selective training remediation | Unified missing-manifest schema + selective execution | 7, 8, 9 |
| 1J | Required-but-unused runtime fields | Wire-or-remove required runtime fields | 2, 8, 9 |
| 1K | Non-canonical target fallback | DDB-only target resolution for enabled branches | 0, 8, 9 |
| 1L | Runtime payload bloat | Minimal run-varying payload, DDB-owned policy | 0, 2, 8, 9 |

### 1.3 Overall fixing strategy
The safest strategy is:

1. Freeze the target contracts first:
   - Batch Index schema,
   - DPP / MLP schema,
   - S3 prefix schema,
   - minimal runtime parameter sets.
2. Refactor shared infrastructure before flows:
   - Batch Index loaders/writers,
   - prefix builders,
   - code-location resolution,
   - runtime config models.
3. Refactor father RT flow first (canonical writer of Batch Index + derived S3 prefixes).
4. Refactor all DPP/MLP jobs to use Batch Index for I/O resolution.
5. Refactor monthly FG-B / machine inventory.
6. Refactor backfill.
7. Refactor training.
8. Add contract hardening, regression tests, and migration verification.

This order minimizes chance of downstream task failure due to unstable foundational contracts.

---

## 3. Unified data contracts (Batch Index + DPP + MLP)

### 3.1 Refined Batch Index design
Because DynamoDB has one PK/SK per item, and required access patterns are both:

- lookup by `batch_id`, and
- reverse lookup by `date_partition`,

use multiple item types in one table under the same `project_name` partition.

#### Table PK
- `PK = project_name`

#### Item type A — direct batch lookup
- `SK = <batch_id>`

Purpose: canonical direct lookup for one raw-log batch.

#### Item type B — reverse date lookup
- `SK = <YYYY/MM/dd>#<hh>#<within_hour_run_number>`

Purpose: querying with `begins_with(SK, "<YYYY/MM/dd>#")` returns all batch IDs for that date in deterministic hour/run order.

#### Why this over “also date_partition as SK”
A single item cannot simultaneously have:

- `SK = batch_id`, and
- `SK = date_partition`.

Writing two index items per batch under same `project_name` is the cleanest fit.

Implementation policy for this refactor: this is a greenfield deployment path with no migration step. The Batch Index contract is implemented directly with exactly two items per batch (`<batch_id>` and `<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) and no additional index requirement in this phase.

### 3.2 Refined Batch Index item contents

#### Item A — `<batch_id>`
Required attributes:

- `project_name`
- `batch_id`
- `date_partition = YYYY/MM/dd`
- `hour`
- `within_hour_run_number`
- `etl_ts`
- `org1`
- `org2`
- `raw_parsed_logs_s3_prefix`
- `ml_project_names` (array)
- complete `s3_prefixes` map that covers every project IO contract (DPP/MLP data, code, and artifact locations used by RT, backfill, monthly, and training flows), using canonical keys defined in Section 3.5.4.

Canonical key index for usage references (non-authoritative summary; authoritative definitions remain in Section 3.5.4):
- `s3_prefixes.dpp.delta`
- `s3_prefixes.dpp.pair_counts`
- `s3_prefixes.dpp.fg_a`
- `s3_prefixes.dpp.fg_a_subpaths.features`
- `s3_prefixes.dpp.fg_a_subpaths.metadata`
- `s3_prefixes.dpp.fg_c`
- `s3_prefixes.dpp.machine_inventory`
- `s3_prefixes.dpp.fg_b.machines_manifest`
- `s3_prefixes.dpp.fg_b.machines_unload_for_update`
- `s3_prefixes.dpp.fg_b.machines_base_stats`
- `s3_prefixes.dpp.fg_b.segment_base_stats`
- `s3_prefixes.dpp.code.delta_step`
- `s3_prefixes.dpp.code.fg_a_step`
- `s3_prefixes.dpp.code.pair_counts_step`
- `s3_prefixes.dpp.code.fg_c_step`
- `s3_prefixes.dpp.code.fg_b_step`
- `s3_prefixes.dpp.code.machine_inventory_unload_step`
- `s3_prefixes.dpp.code.backfill_extractor_step`
- `s3_prefixes.dpp.code_metadata.delta_step`
- `s3_prefixes.dpp.code_metadata.fg_a_step`
- `s3_prefixes.dpp.code_metadata.pair_counts_step`
- `s3_prefixes.dpp.code_metadata.fg_c_step`
- `s3_prefixes.dpp.code_metadata.fg_b_step`
- `s3_prefixes.dpp.code_metadata.machine_inventory_unload_step`
- `s3_prefixes.dpp.code_metadata.backfill_extractor_step`
- `s3_prefixes.mlp.<ml_project_name>.predictions`
- `s3_prefixes.mlp.<ml_project_name>.prediction_join`
- `s3_prefixes.mlp.<ml_project_name>.publication`
- `s3_prefixes.mlp.<ml_project_name>.training_events.training_reports`
- `s3_prefixes.mlp.<ml_project_name>.training_events.training_artifacts`
- `s3_prefixes.mlp.<ml_project_name>.production_artifacts.inference_model`
- `s3_prefixes.mlp.<ml_project_name>.code_metadata.inference_step`
- `s3_prefixes.mlp.<ml_project_name>.code_metadata.join_step`
- `s3_prefixes.mlp.<ml_project_name>.code_metadata.training_step`

All consumer components must resolve requested full prefixes through Batch Index lookups under `project_name` + `batch_id` (and `ml_project_name` branch key for MLP entries), and must not rely on schema definitions outside Section 3.5.4.

Lean status attributes:

- `rt_flow_status`
- `backfill_status`
- `last_updated_at`

#### Item B — `<YYYY/MM/dd>#<hh>#<within_hour_run_number>`
Required attributes:

- `batch_id`
- `date_partition`
- `hour`
- `within_hour_run_number`
- `etl_ts`
- `org1`
- `org2`
- optional compact copy of `raw_parsed_logs_s3_prefix`
- `batch_lookup_sk = <batch_id>` (pointer to item A for full prefix retrieval)

### 3.3 Who writes Batch Index?
Both must write:

- RT father flow,
- backfill flow (when reconstructing historical batches).

Target rule: any flow producing processed data from raw logs must ensure both index item types exist and all DPP/MLP prefixes are precomputed/written.

### 3.4 Pre-write vs update behavior
Correct target behavior:

- RT father flow pre-writes all needed I/O prefixes **in advance**,
- not prefix updates after each step.

This is possible from:

- raw prefix (payload),
- date partition (raw prefix),
- hour/run number (ETL timestamp),
- DPP/MLP root prefixes (tables).

Later updates should be status/timestamps/failure info only.

Backfill write behavior (including Redshift fallback):
- Backfill computes missing batch candidates from planner/extractor manifest.
- For each reconstructed batch, it derives `date_partition`, `hour`, `within_hour_run_number`, and branch `ml_project_names`.
- It resolves DPP/MLP roots and writes Item A (`SK=<batch_id>`) with full `s3_prefixes` map, plus Item B (`SK=<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) with `batch_lookup_sk`.
- If ingestion data is missing and Redshift UNLOAD fallback is used, the same write contract applies; only the source provenance/status fields differ (`source_mode=redshift_unload_fallback`).

---

### 3.5 DPP and MLP table roles

#### 3.5.1 DPP role
DPP should provide:

- `project_name -> ml_project_name(s)` linkage,
- DPP root S3 prefixes,
- raw ingestion metadata (if needed),
- Redshift fallback/unload config for data-processing flows,
- data-processing orchestration runtime parameters.

DPP should own:

- delta root prefix,
- pair-counts root prefix,
- FG-A root prefix,
- FG-B root prefix family (machines manifest, machines unload for update, machine-base stats, segment-base stats),
- FG-C root prefix,
- machine inventory root prefix,
- Redshift unload source config for DPP/backfill/machine inventory,
- code root prefixes for DPP step scripts (if code in S3).

Cold-start control fields (stored in DPP `project_parameters.spec.defaults`) required for first deployment:

- `InitialDeployment` (bool; defaults `true` on first deployment),
- `ColdStartBackfillStartTsIso`,
- `ColdStartBackfillEndTsIso`,
- `ColdStartReferenceTimeIso`,
- `ColdStartCompletionStatus` (`pending|running|completed|failed`),
- `ColdStartCompletedAtIso` (nullable),
- `ColdStartRunId` (nullable),
- `ColdStartFailureReason` (nullable).

#### 3.5.2 MLP role
MLP should provide:

- root S3 prefixes keyed by `ml_project_name`,
- inference output roots,
- publication roots/targets,
- training-run roots,
- training-report roots,
- model-artifact roots,
- endpoint/deployment metadata,
- model-definition metadata (including `model_type` and model-family training config),
- evaluation policy metadata (metrics, thresholds, directions, weighting, and decision rules) resolved by `ml_project_name` (with optional internal version fields).

Mandatory ownership rule for this refactor:
- Batch Index remains batch-scoped (resolved prefixes + runtime execution attributes).
- MLP is the default source of truth for model-definition and evaluation-policy contracts.
- Consumers must not read `model_type` or evaluation metrics/thresholds from Batch Index except as an explicitly documented emergency override path.

#### 3.5.3 Lean table structure recommendation
Keep split-table philosophy with clearer item families.

DPP families:

- linkage,
- s3_roots,
- job_specs,
- unload_sources.

MLP families:

- s3_roots,
- inference,
- publication,
- training,
- deployment.

#### 3.5.4 Complete DynamoDB table JSON contract examples
#### Batch Index table (example items)
```json
{
  "table_name": "batch_index",
  "partition_key": {"name": "PK", "type": "S", "value_example": "<project_name>"},
  "sort_key": {"name": "SK", "type": "S"},
  "item_type_A_example": {
    "PK": "<project_name>",
    "SK": "<batch_id>",
    "batch_id": "<batch_id>",
    "date_partition": "<YYYY/MM/dd>",
    "hour": "<hh>",
    "within_hour_run_number": "<within_hour_run_number>",
    "etl_ts": "<YYYY-MM-DDThh:mm:ssZ>",
    "org1": "<org1>",
    "org2": "<org2>",
    "raw_parsed_logs_s3_prefix": "s3://<ingestion_bucket>/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<batch_id>/",
    "ml_project_names": ["<ml_project_name_1>", "<ml_project_name_2>"],
    "s3_prefixes": {
      "dpp": {
        "delta": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/delta/part-00000.parquet",
        "pair_counts": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/pair_counts/part-00000.parquet",
        "fg_a": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_a/features/part-00000.parquet",
        "fg_a_subpaths": {
          "features": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_a/features/part-00000.parquet",
          "metadata": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_a/metadata/schema.json"
        },
        "fg_c": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_c/part-00000.parquet",
        "machine_inventory": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/machine_inventory/snapshot.parquet",
        "fg_b": {
          "machines_manifest": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_b/machines_manifest/manifest.json",
          "machines_unload_for_update": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_b/machines_unload_for_update/unload_000.parquet",
          "machines_base_stats": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_b/machines_base_stats/part-00000.parquet",
          "segment_base_stats": "s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/fg_b/segment_base_stats/part-00000.parquet"
        },
        "code": {
          "delta_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/DeltaBuilderStep/run_delta_builder.py",
          "fg_a_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/FGABuilderStep/run_fg_a_builder.py",
          "pair_counts_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/PairCountsBuilderStep/run_pair_counts_builder.py",
          "fg_c_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/FGCCorrBuilderStep/run_fg_c_builder.py",
          "fg_b_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/monthly/FGBaselineBuilderStep/run_fg_b_builder.py",
          "machine_inventory_unload_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/monthly/MachineInventoryUnloadStep/run_machine_inventory_unload.py",
          "backfill_extractor_step": "s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/backfill/HistoricalWindowsExtractorStep/run_historical_windows_extractor.py"
        },
        "code_metadata": {
          "delta_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
          "fg_a_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
          "pair_counts_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
          "fg_c_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
          "fg_b_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
          "machine_inventory_unload_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
          "backfill_extractor_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"}
        }
      },
      "mlp": {
        "<ml_project_name>": {
          "predictions": "s3://<ml_bucket>/mlp/<ml_project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/predictions/part-00000.parquet",
          "prediction_join": "s3://<ml_bucket>/mlp/<ml_project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/prediction_join/part-00000.parquet",
          "publication": "s3://<ml_bucket>/mlp/<ml_project_name>/<org1>/<org2>/<YYYY>/<MM>/<dd>/<hh>/<within_hour_run_number>/publication/publication_payload.json",
          "training_events": {
            "training_reports": "s3://<ml_bucket>/mlp/<ml_project_name>/training/events/<run_id_from_training_script>/reports/final_training_report.json",
            "training_artifacts": "s3://<ml_bucket>/mlp/<ml_project_name>/training/events/<run_id_from_training_script>/artifacts/model/model.tar.gz"
          },
          "production_artifacts": {
            "inference_model": "s3://<ml_bucket>/mlp/<ml_project_name>/production/model/model.tar.gz",
            "model_creation_script": "s3://<ml_bucket>/mlp/<ml_project_name>/production/model_creation_script/create_model.py",
            "production_evaluation_scores": "s3://<ml_bucket>/mlp/<ml_project_name>/production/evaluation_scores/latest_model_eval_summary.json"
          },
          "code": {
            "inference_step": "s3://<ml_bucket>/code/mlp/<ml_project_name>/<feature_spec_version>/inference/InferencePredictionsStep/run_inference_predictions.py",
            "join_step": "s3://<ml_bucket>/code/mlp/<ml_project_name>/<feature_spec_version>/publication/PredictionFeatureJoinStep/run_prediction_feature_join.py",
            "training_step": "s3://<ml_bucket>/code/mlp/<ml_project_name>/<feature_spec_version>/training/IFTrainingStep/run_if_training.py"
          },
          "code_metadata": {
            "inference_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
            "join_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"},
            "training_step": {"artifact_mode": "single_file", "artifact_build_id": "<build_id>", "artifact_sha256": "<sha256>"}
          }
        }
      }
    },
    "rt_flow_status": "<status>",
    "backfill_status": "<status>",
    "source_mode": "<ingestion_or_redshift_unload_fallback>",
    "last_updated_at": "<YYYY-MM-DDThh:mm:ssZ>"
  },
  "item_type_B_example": {
    "PK": "<project_name>",
    "SK": "<YYYY/MM/dd>#<hh>#<within_hour_run_number>",
    "batch_id": "<batch_id>",
    "batch_lookup_sk": "<batch_id>",
    "date_partition": "<YYYY/MM/dd>",
    "hour": "<hh>",
    "within_hour_run_number": "<within_hour_run_number>",
    "etl_ts": "<YYYY-MM-DDThh:mm:ssZ>",
    "org1": "<org1>",
    "org2": "<org2>"
  }
}
```

### 3.6 Single-source-of-truth contract governance (mandatory)
To remove schema ambiguity, this plan defines a single authoritative source:

- **Authoritative schema source:** Section 3.5.4 only.
- Any schema-like representation elsewhere in this document must reference Section 3.5.4 keys and must not redefine keys/field names.
- Change control rule: any key addition/removal/rename must be updated first in 3.5.4 and then propagated to dependent sections (5.x, 6.x, 7 tasks, 11 traceability, 12 acceptance checks).
- Validation rule: CI/doc validation for this plan must fail if a key appears in usage tables without a matching authoritative definition in 3.5.4.

#### DPP table (example items)
```json
{
  "table_name": "dpp_parameters",
  "partition_key": {"name": "PK", "type": "S", "value_example": "<project_name>"},
  "sort_key": {"name": "SK", "type": "S"},
  "items": {
    "linkage": {"PK": "<project_name>", "SK": "LINKAGE", "ml_project_names": ["<ml_project_name_1>", "<ml_project_name_2>"]},
    "s3_roots": {
      "PK": "<project_name>",
      "SK": "S3_ROOTS",
      "delta_root": "s3://<ml_bucket>/dpp/<project_name>/delta/",
      "pair_counts_root": "s3://<ml_bucket>/dpp/<project_name>/pair_counts/",
      "fg_a_root": "s3://<ml_bucket>/dpp/<project_name>/fg_a/",
      "fg_b_root": "s3://<ml_bucket>/dpp/<project_name>/fg_b/",
      "fg_c_root": "s3://<ml_bucket>/dpp/<project_name>/fg_c/",
      "machine_inventory_root": "s3://<ml_bucket>/dpp/<project_name>/machine_inventory/"
    },
    "unload_sources": {
      "PK": "<project_name>",
      "SK": "UNLOAD_SOURCES",
      "cluster": "<cluster>",
      "database": "<database>",
      "secret_arn": "<secret_arn>",
      "iam_role_arn": "<iam_role_arn>",
      "schema": "<schema>",
      "table": "<table_or_view>"
    }
  }
}
```

#### MLP table (example items)
```json
{
  "table_name": "ml_projects_parameters",
  "partition_key": {"name": "PK", "type": "S", "value_example": "<ml_project_name>"},
  "sort_key": {"name": "SK", "type": "S"},
  "items": {
    "s3_roots": {"PK": "<ml_project_name>", "SK": "S3_ROOTS", "predictions_root": "s3://<ml_bucket>/mlp/<ml_project_name>/predictions/", "publication_root": "s3://<ml_bucket>/mlp/<ml_project_name>/publication/"},
    "model_definition": {"PK": "<ml_project_name>", "SK": "MODEL_DEFINITION", "model_type": "<model_type>", "training_family_config": {"...": "<value>"}},
    "evaluation_policy": {"PK": "<ml_project_name>", "SK": "EVALUATION_POLICY", "metrics": [{"name": "<metric_name>", "threshold": "<threshold>", "direction": "<gte_or_lte>", "weight": "<weight>"}], "decision_rule": {"type": "<rule_type>", "min_weighted_pass_ratio": "<ratio>"}},
    "deployment": {"PK": "<ml_project_name>", "SK": "DEPLOYMENT", "endpoint_name": "<endpoint_name>", "model_package_arn": "<model_package_arn>"}
  }
}
```

---

## 5. Refined S3 schema (including code-file layout)

### 5.1 Raw ingestion bucket (accepted as-is)

```text
s3://<ingestion_bucket_name>/<project_name>/<org1>/<org2>/YYYY/MM/dd/<batch_id>/<log_file_1.json.gz> ... <log_file_n.json.gz>
```

### 5.2 DPP data layout

```text
s3://<ml_bucket>/dpp/<project_name>/<org1>/<org2>/YYYY/MM/dd/hh/<within_hour_run_number>/
  delta/
  pair_counts/
  fg_a/
    features/
    metadata/
  fg_c/
  fg_b/
    machines_manifest/
    machines_unload_for_update/
    machines_base_stats/
    segment_base_stats/
  machine_inventory/
```

Reasons:

- no duplication across MLPs,
- deterministic,
- easy RT/backfill pre-build,
- aligned with date/hour/run-number requirement.

### 5.3 MLP data/artifact layout

```text
s3://<ml_bucket>/mlp/<ml_project_name>/<org1>/<org2>/YYYY/MM/dd/hh/<within_hour_run_number>/
  predictions/
  prediction_join/
  publication/
```

Training and production artifacts are not batch-granularity assets and must be managed separately:

```text
s3://<ml_bucket>/mlp/<ml_project_name>/training/events/<run_id_from_training_script>/
  reports/final_training_report.json
  reports/failure_report.json
  reports/preflight_failure_context.json
  reports/history_planner.json
  reports/evaluation/<window_id>/scores.json
  artifacts/model/model.pkl
  artifacts/model/model.tar.gz
  artifacts/preprocessing/scaler_params.json
  artifacts/preprocessing/outlier_params.json
  artifacts/preprocessing/feature_mask.json

s3://<ml_bucket>/mlp/<ml_project_name>/production/
  model/model.tar.gz
  evaluation_scores/latest_model_eval_summary.json
```

### 5.4 Code-file layout
Code should not be duplicated under run folders.

Production requirement for this refactor: each runtime step must receive a **single-file Python artifact** published to the canonical code prefix and referenced from Batch Index (`s3_prefixes.*.code.*`). The repository remains multi-file for development; the deployment artifact is generated per step.

Schema-C execution policy (authoritative):

- Use SageMaker-provided processing images for step containers.
- Publish one generated single-file artifact per step into canonical step code prefixes.
- Resolve the artifact via existing `code_prefix_s3 + entry_script` contract resolution.
- Record deterministic artifact metadata per step (`artifact_mode=single_file`, `artifact_build_id`, `artifact_sha256`) under `s3_prefixes.*.code_metadata.*`.
- Treat image dependency set + single-file artifact hash as the runtime reproducibility boundary.

Required explicit code object entries:

- DPP pipeline scripts:
  - `s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/DeltaBuilderStep/run_delta_builder.py`
  - `s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/FGABuilderStep/run_fg_a_builder.py`
  - `s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/PairCountsBuilderStep/run_pair_counts_builder.py`
  - `s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/feature_creation/FGCCorrBuilderStep/run_fg_c_builder.py`
  - `s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/monthly/MachineInventoryUnloadStep/run_machine_inventory_unload.py`
  - `s3://<ml_bucket>/code/dpp/<project_name>/<feature_spec_version>/backfill/HistoricalWindowsExtractorStep/run_historical_windows_extractor.py`

- MLP pipeline scripts:
  - `s3://<ml_bucket>/code/mlp/<ml_project_name>/<feature_spec_version>/inference/InferencePredictionsStep/run_inference_predictions.py`
  - `s3://<ml_bucket>/code/mlp/<ml_project_name>/<feature_spec_version>/publication/PredictionFeatureJoinStep/run_prediction_feature_join.py`
  - `s3://<ml_bucket>/code/mlp/<ml_project_name>/<feature_spec_version>/training/IFTrainingStep/run_if_training.py`

- Step Functions configuration JSON objects:
  - `s3://<ml_bucket>/code/orchestration/<project_name>/<feature_spec_version>/sfn_RT_features_inference_publication.json`
  - `s3://<ml_bucket>/code/orchestration/<project_name>/<feature_spec_version>/sfn_prediction_publication.json`
  - `s3://<ml_bucket>/code/orchestration/<project_name>/<feature_spec_version>/sfn_monthly_fg_b_baselines.json`
  - `s3://<ml_bucket>/code/orchestration/<project_name>/<feature_spec_version>/sfn_training_orchestrator.json`
  - `s3://<ml_bucket>/code/orchestration/<project_name>/<feature_spec_version>/sfn_backfill_reprocessing.json`

- Required generated artifact metadata entries (stored in Batch Index under `code_metadata`):
  - `artifact_mode = single_file`
  - `artifact_build_id`
  - `artifact_sha256`
  - optional `artifact_generated_at_utc`

Runtime dependency resolution requirements (for restricted-network environments):

- This concern is handled natively by Schema-C contract usage in this plan: step code is resolved from Batch Index-backed S3 prefixes, and runtime execution uses the configured processing image path for each step.
- No additional schema beyond canonical Batch Index + step code prefixes is required in this document for restricted-network handling.

Benefits:

- avoids per-run duplication,
- enables stable builder resolution from DPP/MLP config with explicit object-level keys,
- keeps DPP/MLP root ownership lean while allowing RT/backfill to materialize full per-batch prefixes into Batch Index.

Updated decision for this refactor: Batch Index must store all required full prefixes (data, artifacts, and code pointers) so every runtime consumer can resolve required IO/code locations from one lookup path. DPP/MLP remain root owners, while RT/Backfill materialize full per-batch/per-ML-project prefixes into Batch Index.

### 5.5 S3 schema coverage matrix (required validation)
Every defined IO operation in code/config must map to an explicit prefix above. Implementors must validate this before task signoff.

| IO operation | Writer(s) | Reader(s) | Required S3 entry example | Batch Index key |
|---|---|---|---|---|
| Delta output | Delta builder | FG-A, FG-C | `.../delta/part-00000.parquet` | `s3_prefixes.dpp.delta` |
| Pair counts output | Pair-counts builder | FG-C, FG-B | `.../pair_counts/part-00000.parquet` | `s3_prefixes.dpp.pair_counts` |
| FG-A output | FG-A builder | FG-C, inference feature assembly | `.../fg_a/features/part-00000.parquet` | `s3_prefixes.dpp.fg_a` and `s3_prefixes.dpp.fg_a_subpaths.features` |
| FG-C output | FG-C builder | Inference predictions | `.../fg_c/part-00000.parquet` | `s3_prefixes.dpp.fg_c` |
| Machine inventory snapshot | Machine inventory unload | FG-B baseline build | `.../machine_inventory/snapshot.parquet` | `s3_prefixes.dpp.machine_inventory` |
| FG-B machine manifest | Machine inventory + FG-B prep | FG-B builder | `.../fg_b/machines_manifest/manifest.json` | `s3_prefixes.dpp.fg_b.machines_manifest` |
| FG-B unload for update | Monthly unload step | FG-B builder | `.../fg_b/machines_unload_for_update/unload_000.parquet` | `s3_prefixes.dpp.fg_b.machines_unload_for_update` |
| FG-B machine base stats | FG-B builder | Training/inference feature join as needed | `.../fg_b/machines_base_stats/part-00000.parquet` | `s3_prefixes.dpp.fg_b.machines_base_stats` |
| FG-B segment base stats | FG-B builder | Training/inference feature join as needed | `.../fg_b/segment_base_stats/part-00000.parquet` | `s3_prefixes.dpp.fg_b.segment_base_stats` |
| Inference predictions | Inference step | Prediction join/publication | `.../predictions/part-00000.parquet` | `s3_prefixes.mlp.<ml_project_name>.predictions` |
| Prediction join output | Join step | Publication | `.../prediction_join/part-00000.parquet` | `s3_prefixes.mlp.<ml_project_name>.prediction_join` |
| Publication output | Publication step | downstream consumers | `.../publication/publication_payload.json` | `s3_prefixes.mlp.<ml_project_name>.publication` |
| Training reports | IF training flow | model governance/alerts | `.../training/events/<run_id_from_training_script>/reports/final_training_report.json` | `s3_prefixes.mlp.<ml_project_name>.training_events.training_reports` |
| Training artifacts | IF training flow | model registration/promotion | `.../training/events/<run_id_from_training_script>/artifacts/model/model.tar.gz` | `s3_prefixes.mlp.<ml_project_name>.training_events.training_artifacts` |
| Production artifacts | Promotion step | Inference runtime | `.../production/model/model.tar.gz` | `s3_prefixes.mlp.<ml_project_name>.production_artifacts.inference_model` |
| Step single-file artifact publish | Build/deploy process | Pipeline builders + ProcessingStep runtime | `.../code/<family>/<scope>/<feature_spec_version>/<step>/run_<step>.py` | `s3_prefixes.<family>.<...>.code.<step_key>` |
| Step code metadata publish | Build/deploy process | Runtime validators + release gates | Batch Index metadata object | `s3_prefixes.<family>.<...>.code_metadata.<step_key>.*` |
| Step artifact fetch at runtime | ProcessingStep launcher | Step execution container | `.../code/<...>/run_<step>.py` | `s3_prefixes.<family>.<...>.code.<step_key>` |

### 5.6 Mandatory full-codebase IO inspection procedure (no partial compliance allowed)
This procedure is mandatory and must be executed before any refactor task is considered complete:

1. Enumerate every IO operation from the project codebase (`src/ndr/processing`, `src/ndr/scripts`, `src/ndr/pipeline`, deployment/bootstrap scripts), including:
   - S3 reads/writes,
   - Redshift UNLOAD/COPY interactions,
   - DynamoDB reads/writes,
   - Step Functions and SageMaker pipeline execution payload I/O.
2. Materialize that enumeration as a checked-in inventory table in this plan (Section 5.7).
3. For each enumerated IO operation, record:
   - source file + function,
   - operation type (read/write),
   - artifact/output type,
   - required S3 object key pattern,
   - required Batch Index key path (or explicit N/A with justification),
   - producer and consumer.
4. Fail validation if any IO operation lacks a designated S3 entry and contract mapping.
5. Fail validation if any designated S3 entry is not backed by at least one concrete producer in code.

### 5.7 Codebase-derived IO inventory baseline (from current inspection; must be expanded to completion)
| File / function area | IO operation | Direction | S3 object / target | Batch Index key linkage |
|---|---|---|---|---|
| `delta_builder_job.py` | Delta parquet output | Write | `.../delta/part-*.parquet` | `s3_prefixes.dpp.delta` |
| `fg_a_builder_job.py` | Delta input | Read | `.../delta/part-*.parquet` | `s3_prefixes.dpp.delta` |
| `fg_a_builder_job.py` | FG-A features output | Write | `.../fg_a/features/part-*.parquet` | `s3_prefixes.dpp.fg_a`, `s3_prefixes.dpp.fg_a_subpaths.features` |
| `pair_counts_builder_job.py` | Pair-counts output | Write | `.../pair_counts/part-*.parquet` | `s3_prefixes.dpp.pair_counts` |
| `fg_c_builder_job.py` | FG-C output | Write | `.../fg_c/part-*.parquet` | `s3_prefixes.dpp.fg_c` |
| `machine_inventory_unload_job.py` | Redshift UNLOAD data | Write | `.../machine_inventory/snapshot.parquet` or unload partition objects | `s3_prefixes.dpp.machine_inventory` |
| `fg_b_builder_job.py` | FG-B machine/segment stats | Write | `.../fg_b/machines_base_stats/part-*.parquet`, `.../fg_b/segment_base_stats/part-*.parquet` | `s3_prefixes.dpp.fg_b.*` |
| `inference_predictions_job.py` | Predictions output | Write | `.../predictions/part-*.parquet` | `s3_prefixes.mlp.<ml_project_name>.predictions` |
| `prediction_feature_join_job.py` | Join output | Write | `.../prediction_join/part-*.parquet` | `s3_prefixes.mlp.<ml_project_name>.prediction_join` |
| `prediction_feature_join_job.py` | Optional Redshift copy staging | Write/read staging | `.../prediction_feature_join/...` | `s3_prefixes.mlp.<ml_project_name>.prediction_join` |
| `if_training_job.py` | Training reports | Write | `.../training/events/<run_id_from_training_script>/reports/*.json` | `s3_prefixes.mlp.<ml_project_name>.training_events.training_reports` |
| `if_training_job.py` | Training artifacts + preprocessing | Write | `.../training/events/<run_id_from_training_script>/artifacts/model/model.tar.gz`, `.../preprocessing/*.json` | `s3_prefixes.mlp.<ml_project_name>.training_events.training_artifacts` |
| `if_training_job.py` | Production model copy | Write | `.../production/model/model.tar.gz` | `s3_prefixes.mlp.<ml_project_name>.production_artifacts.inference_model` |
| `historical_windows_extractor_job.py` | Historical windows manifest | Write | `.../backfill/windows/historical_windows/*.jsonl` | must map into Batch Index write flow inputs |
| Backfill execution flow (target state) | Reconstructed batch Item A/B writes | Write (DDB) | N/A S3 direct; uses resolved S3 outputs | item A + item B keys with full `s3_prefixes` map |

### 5.8 Conformance gate for code packaging completeness
For each pipeline step, verify both are true:
1. Entry script object key exists in S3 code map.
2. Batch Index `code_metadata` entry exists with `artifact_mode=single_file`, `artifact_build_id`, and `artifact_sha256`.

A step fails release gating if any required artifact URI/metadata is missing.

### 5.9 Closed-environment handling
Closed-environment handling is already covered by the canonical Batch Index + S3 step code contract in this plan. No additional compatibility metadata or side-processes are required in this document.

---

## 6. Unified expected behaviour by flow and component (final target state)

This section is the primary end-state reference for implementors. It unifies flow-level orchestration expectations and runtime behaviour expectations that task implementors must satisfy.

### 6.1 Flow 1 — RT feature creation → inference → publication

#### Corrected end state
Father RT SF receives:

- `project_name`
- `batch_id`
- `raw_parsed_logs_s3_prefix`
- `etl_ts`

Then:

1. Resolve `ml_project_names` (array of strings) from DPP as the canonical ML-project linkage.
2. Normalize/validate that array before any MLP-specific reads (non-empty, strings-only, deduplicated).
3. Derive `date_partition`, `hour`, `within_hour_run_number`, `org1`, `org2`.
4. Resolve DPP/MLP roots.
5. Construct full DPP/MLP batch-run prefixes.
6. Write prefix map into Batch Index **before** launching pipelines.
7. Fan out by iterating each element of `ml_project_names` and executing downstream flow components per branch context.
8. Each child resolves exact full paths from Batch Index.

#### Important file direction
- `sfn_RT_features_inference_publication.json`
  - reduce payload parsing,
  - remove direct ML-project payload selection,
  - add DPP-resolved `ml_project_names` Map fan-out,
  - pre-write full prefix maps into Batch Index,
  - pass explicit branch context into nested publication flow.

- `sagemaker_pipeline_definitions_feature_creation.py` (genericized target naming)
  - runtime semantics must remain cadence- and project-generic.

#### RT-triggered pipeline chain (must be explicit)
1. Feature creation pipeline: Delta → FG-A → PairCounts → FG-C (all IO via Batch Index contract).
2. Inference predictions pipeline: resolve branch (`ml_project_name`), consume FG outputs, write predictions.
3. Publication pipeline: run prediction join + publication using branch context and Batch Index MLP branch keys.

#### Flow-1 scripts/jobs to refactor
- `run_delta_builder.py`
- `delta_builder_job.py`
- `run_fg_a_builder.py`
- `fg_a_builder_job.py`
- `run_pair_counts_builder.py`
- `pair_counts_builder_job.py`
- `run_fg_c_builder.py`
- `fg_c_builder_job.py`
- `run_inference_predictions.py`
- `inference_predictions_job.py`
- nested publication SF
- `run_prediction_feature_join.py`
- `prediction_feature_join_job.py`

#### Runtime parameter guidance
- DPP jobs: `project_name`, `batch_id`, and `ml_project_name` only if needed for branch-aware lookup.
- MLP jobs: `project_name`, `batch_id`, `ml_project_name`.
- Cross-component full timestamps should not be required as interface parameters; components derive needed times from Batch Index attributes (`etl_ts`, `date_partition`, `hour`, `within_hour_run_number`) when needed.

#### Orchestration ownership rule (lean mandatory)
- Step Functions own cross-pipeline ordering (feature creation -> inference -> join/publication; monthly machine-update -> FG-B).
- Pipeline/job code must not introduce new orchestration behavior; jobs only execute their local contract.
- Keep runtime parameter count minimal: only pass values a step cannot deterministically resolve from Batch Index/DPP/MLP contracts.

#### Required corrections
- PairCounts: always produce sufficient segment stats (including target-side as needed).
- FG-C: resolve exact batch paths from Batch Index, not timestamps.
- Publication: father flow must pass explicit branch context.

#### Lean RT feature-creation fix plan (mandatory)
The RT feature-creation chain (Delta -> FG-A -> PairCounts -> FG-C) must implement the following in this exact order:

1. **RT-0 Contract hardening (DDB-only runtime resolution)**
   - Resolve step code URI, data prefixes, join keys, and feature contracts from DDB.
   - Prohibit placeholder/default contract resolution (`<required:...>`) at runtime.
   - Allow trigger payload to carry only run-varying values by design.

2. **RT-1 Batch Index authority**
   - RT SF must write `batch_id` and `raw_parsed_logs_s3_prefix` into Batch Index on ingest trigger.
   - Downstream RT steps must consume batch identity/path pointers from Batch Index rather than reconstructed timestamp paths.
   - Enforce idempotent upsert semantics for repeated trigger events.

3. **RT-2 Delta contract alignment**
   - Align Delta partition key contract with produced columns (`dt`,`hh`,`mm`) and remove drift (`hour` mismatch).
   - Add contract gate that validates produced partition columns against DDB contract.

4. **RT-3 Delta->FG-A mini-batch strictness**
   - Keep one contract only:
     - preferred: Delta includes `mini_batch_id` in output contract, or
     - explicit compatibility mode from DDB (temporary only).
   - FG-A must fail with explicit context when strict contract is violated.

5. **RT-4 Exact-batch read behavior**
   - Replace broad-prefix read+filter patterns with exact Batch Index-resolved prefixes.
   - Keep filters only as defensive checks, not as primary batch scoping mechanism.

6. **RT-5 FG-C correctness hardening**
   - Missing FG-B host baselines must produce fail-fast/remediation signal (not silent empty success).
   - FG-C host join keys must match FG-B host baseline granularity contract.
   - Segment fallback join must reject under-specified key sets (no silent key dropping).

7. **RT-6 PairCounts placement review**
   - Keep FG-A -> PairCounts dependency only if explicitly required by contract.
   - If no hard data dependency exists, simplify dependency to reduce RT critical-path latency.

### 6.2 Flow 2 — Monthly machine inventory refresh + FG-B baseline

#### Key corrections
- Scripts resolve DPP/MLP/Batch Index paths as relevant.
- Use `reference_month` format: `YYYY/MM` (not timestamp).
- Monthly baseline flow should not require a separate `mode` parameter in normal operation; monthly behavior is deterministic by `reference_month` + project context.
- Machine inventory remains an independent pipeline, but the FG-B monthly Step Function must always trigger it first and gate FG-B execution on its success.

#### Lean cold-start fixes to integrate (simple-by-design)
1. **Strict two-step orchestration only:** `MachineInventoryUnload` then `FGBaselineBuilder`; no extra branching for normal monthly flow.
2. **Single authoritative mapping input for FG-B:** FG-B consumes one canonical machine-mapping prefix produced by the machine-update flow (with one explicit fallback key for backward compatibility).
3. **Freshness gate:** before FG-B, validate machine-update artifact freshness for target `reference_month`; fail fast when stale.
4. **No silent key degradation in FG-C segment fallback:** require minimum segment join keys and fail if missing.
5. **Join-key alignment contract:** FG-C host-baseline joins must use the same granularity dimensions FG-B uses to build host baselines.
6. **Scope clarity:** FG-C is the cold/warm baseline-routing component; FG-A and Pair-Counts remain baseline-agnostic unless explicitly expanded in a later scoped task.
7. **Minimal observability:** emit only required counters/signals (`machine_update_ok`, `cold_rows`, `warm_rows`, `fallback_rows`) to keep operations diagnosable without adding complex telemetry surface area.

#### Files to change
- `sfn_monthly_fg_b_baselines.json`
- `sagemaker_pipeline_definitions_unified_with_fgc.py`
- `run_machine_inventory_unload.py`
- `machine_inventory_unload_job.py`
- `run_fg_b_builder.py`
- `fg_b_builder_job.py`

#### Redshift parameter ownership
Current machine inventory parameters read from JobSpec; target ownership should be DPP by default for machine inventory and raw enrichment, and for backfill fallback.

#### Inventory consistency requirement
Refactor must support:

- monthly snapshot creation,
- inventory merge/update semantics,
- transitions such as:
  - insufficient history → sufficient history,
  - non-persistent flag updates,
  - first-seen machine emergence,
  - metadata refresh.

### 6.3 Flow 3 — IF training
Training must support:

- scheduled and script-triggered execution,
- 3 modes,
- `project_name + ml_project_name`,
- evaluation schema,
- Batch Index-driven path resolution,
- feature selection,
- tuning,
- experiments,
- documentation,
- automatic backfill on missing features.

#### Training modes
- Mode A: `training_and_evaluation_only`
- Mode B: `training_and_evaluation_then_prod_training`
- Mode C: `prod_training_only`

#### Evaluation gate policy
Two supported ways:

1. Default path: resolve model-definition + evaluation policy from MLP using `ml_project_name`.
2. Explicit `evaluation_policy` payload at runtime (controlled override path only).

Default recommendation: `ml_project_name`-based MLP resolution (repeatable, auditable, scheduler-friendly).

Evaluation policy schema (MLP entry) should support multi-metric thresholds and weighted decision rules, for example:

```json
{
  "evaluation_policy_id": "if_eval_default_v1",
  "metrics": [
    {"name": "precision_at_k", "threshold": 0.82, "direction": "gte", "weight": 0.5},
    {"name": "false_positive_rate", "threshold": 0.03, "direction": "lte", "weight": 0.3},
    {"name": "alert_volume_delta", "threshold": 0.10, "direction": "lte", "weight": 0.2}
  ],
  "decision_rule": {"type": "weighted_score", "min_weighted_pass_ratio": 0.8}
}
```

Runtime trigger payload should pass only run-varying values (`project_name`, `ml_project_name`, `training_mode`, time-window ranges). MLP should provide model/evaluation contracts by `ml_project_name`.

#### Files to refactor
- `sfn_training_orchestrator.json`
- `sagemaker_pipeline_definitions_if_training.py`
- `run_if_training.py`
- `if_training_spec.py`
- `if_training_job.py`

#### Target behavior
Training should resolve prefixes from Batch Index + DPP/MLP, verify feature windows, call backfill when needed, run train/eval, optionally gate prod training, store artifacts/reports in MLP roots, publish production metadata.

### 6.4 Flow 4 — Backfill / reprocessing
Requirements integrated:

- unload fallback when requested date partition/batch IDs are not in ingestion bucket,
- backfill reusable by all other flows,
- both self-detect and caller-guided missing-window modes.

#### Modes
1. **Self-detect**: caller provides project + range + purpose; backfill detects missing artifacts.
2. **Caller-guided**: caller provides explicit missing ranges/batch IDs/artifact families.
3. **Cold-start bootstrap**: activated when DPP `InitialDeployment=true`; backfill executes initial historical reconstruction and baseline/materialization chain before normal RT inference publication is allowed.

#### Cold-start bootstrap behavior (must reuse Backfill flow; no separate flow)
- Read `InitialDeployment` and cold-start window/default fields from DPP `project_parameters.spec.defaults`.
- If `InitialDeployment=true`, force backfill execution in cold-start mode using `ColdStartBackfillStartTsIso`/`ColdStartBackfillEndTsIso`.
- Materialize required reconstructed batch prefixes into Batch Index (Item A + Item B) for each rebuilt window.
- Run full dependency path completion before enabling normal RT publication:
  machine inventory unload/update → Delta → FG-A → Pair-Counts → FG-B → FG-C → inference/publication readiness.
- Flip `InitialDeployment` to `false` only after all cold-start gates pass.

#### Selective backfill trigger policy (mandatory)
Backfill must decide per flow/artifact family whether work is needed *before* launching that family.

Required behavior:
- Compute missing ranges per artifact family (not one global range) using Batch Index + target output presence checks.
- Trigger only required families and only for required date ranges.
- Keep family-level dependency ordering (for example FG-C may run only for windows whose FG-A/FG-B prerequisites are present or scheduled in the same run).
- Skip families/ranges already complete.

Two required operating profiles:
1. **Initial deployment (`InitialDeployment=true`)**: expect most/all families missing; planner should produce near-full range coverage.
2. **Targeted recovery (for example training-triggered backfill)**: planner may produce sparse family/range sets (for example FG-C-only windows) and must not launch unnecessary families.

Cold-start mandatory gates:
1. FG-B baseline artifacts exist for required horizons.
2. FG-B metadata required for FG-C cold-start routing exists (including `is_cold_start` routing signal path).
3. FG-C outputs for bootstrap windows are non-empty and contract-valid.
4. First inference/publication prerequisites are present (model endpoint/config non-placeholder and feature inputs resolvable).
5. Failure path keeps `InitialDeployment=true`, records `ColdStartFailureReason`, and blocks normal RT publication.

#### Redshift fallback recommendation
Parameter impact: yes, backfill interface must include either explicit recovery intent flags or inferred fallback mode, plus DPP lookup keys for Redshift source descriptors.
Add DPP-owned source descriptors:

- cluster/database/secret/role,
- schema/table,
- optional project-specific SQL template.

#### Lean unload strategy (mandatory, Option 3 only)
- Use flow-specific unload queries stored in DPP:
  - machine inventory unload query (consumer-aligned to FG-B metadata needs),
  - missing-raw-logs unload query (consumer-aligned to Delta/Pair-Counts inputs).
- Implementors must create/maintain these custom queries as part of the refactor and keep output schemas aligned to downstream consumers.
- Do not introduce a universal query contract for both flows; keep each flow query minimal and purpose-built.
- Unload execution code must stage unloaded rows in RAM or local filesystem of the SageMaker Processing step before downstream stats/features processing.
- Keep runtime parameters lean: pass only run-varying values (flow, ranges, project identifiers); resolve Redshift/table/query descriptors from DPP.

#### Files to refactor
- `sfn_backfill_reprocessing.json`
- `sagemaker_pipeline_definitions_backfill_historical_extractor.py`
- `run_historical_windows_extractor.py`
- `historical_windows_extractor_job.py`
- shared DPP Redshift config loaders (as needed)

### 6.5 Current code-alignment verdicts that require implementation work
The following verdicts are based on direct code inspection and are mandatory to close during implementation:

1. **Machine-update Redshift UNLOAD path:** implemented in `machine_inventory_unload_job.py` via generated UNLOAD SQL + copy stage; must be refactored to Option-3 flow-specific DPP query contract and local RAM/FS staging before downstream processing.
2. **Backfill missing-raw-data Redshift UNLOAD fallback:** not yet implemented as an end-to-end write path in backfill execution code; must be implemented with Option-3 flow-specific DPP query contract, multi-range support, local RAM/FS staging, and outputs mapped to Batch Index `s3_prefixes`.
3. **Backfill write to Batch Index:** not yet implemented as a complete writer contract in backfill execution code; must write both item types (`SK=<batch_id>`, `SK=<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) and full `s3_prefixes` coverage for reconstructed batches.
4. **Training event identifier for S3 writes:** current training code writes with `run_id` (from runtime config) and must be treated as the canonical `training_event_id` contract key in this plan.

---

## 7. Ordered sub-plans (separate Codex Web tasks)

Each task is designed for isolated execution and includes role, dependencies, target files, checks, and completion criteria.

### 7.0 Execution-unit policy (mandatory)
To reduce risk and improve implementation quality, large tasks in this plan are treated as **epics** and must be executed as **separate atomic tasks**.

- Epics: Task 5, Task 7, Task 8.
- Atomic execution units are defined below as Task `5.x`, `7.x`, and `8.x`.
- **Do not execute an epic as one coding task.** Execute each atomic unit separately with its own prompt, implementation, tests, and closure evidence.
- A downstream atomic unit may start only after all prerequisite units are complete and validated.

#### 7.0.1 Atomic execution map
| Execution unit | Parent epic | Prerequisites | Primary purpose |
|---|---|---|---|
| 5.1 | Task 5 | Tasks 0–4 | Delta/FG-A mini-batch and path-contract hardening |
| 5.2 | Task 5 | 5.1 | PairCounts/FG-C contract and sufficiency hardening |
| 5.3 | Task 5 | 5.1, 5.2 | Inference/join/publication consumer-alignment hardening |
| 7.1 | Task 7 | Tasks 0–6 | Backfill planner + manifest contract freeze |
| 7.2 | Task 7 | 7.1 | Extractor-manifest -> map wiring and selective execution |
| 7.3 | Task 7 | 7.1 | Redshift fallback unload path and range execution |
| 7.4 | Task 7 | 7.1, 7.2, 7.3 | Backfill Batch Index writes, status progression, idempotency |
| 8.1 | Task 8 | Tasks 0–7 | Minimal runtime contract (trigger vs DDB ownership) |
| 8.2 | Task 8 | 8.1 | Sequencing correction (`verify->plan->remediate->reverify->train`) |
| 8.3 | Task 8 | 8.1, 8.2 | Unified missing-window manifest contract |
| 8.4 | Task 8 | 8.3 | Selective remediation execution mechanics |
| 8.5 | Task 8 | 8.2, 8.3 | Batch Index training-readiness authority |
| 8.6 | Task 8 | 8.1 | `ml_project_name` propagation and branch isolation |
| 8.7 | Task 8 | 8.1 | Strict DDB-only orchestration target resolution |
| 8.8 | Task 8 | 8.1 | Placeholder-free step code-URI resolution |
| 8.9 | Task 8 | 8.1 | Required runtime-field hygiene (wire-or-remove) |
| 8.10 | Task 8 | 8.1–8.9 | Integration order lock and execution sequencing gate |
| 8.11 | Task 8 | 8.1–8.10 | Training verification bundle and closure gates |

**Mandatory task handoff bundle (must be embedded into every isolated implementation task prompt):**
- Refactor motivation summary (why this refactor exists, target operating model, and safety goals).
- Global misalignment summary (issues 1A–1L from Section 1.2).
- Global strategy/order summary (why this task order is required and what breaks if order is violated).
- Target data contracts summary (Batch Index dual-item schema, DPP/MLP role ownership, S3 full-prefix resolution rules).
- Current-vs-target behaviour summary for the components referenced by this task (from Section 11).
- Non-negotiable constraints summary (no placeholder/env business parameters, Batch Index-first IO resolution, branch-context propagation, acceptance gates from Section 12).
- Issue-to-fix-to-task trace snippet (copy only the issue IDs touched by this task from Section 1.2.1).
- Task-local “definition of done” including exact acceptance tests and fail-fast conditions.

**Mandatory task prompt fields (checklist):**
1. `Task role in overall plan`
2. `Prerequisites already completed`
3. `Context bundle pasted` (all items above)
4. `Exact files in scope`
5. `Required behavioural outcomes`
6. `Known current deviations to fix`
7. `Validation commands and pass criteria`
8. `Rollback/failure handling notes`

**Mandatory task execution packet format (must be present for every task, no exceptions):**
1. **Issue coverage:** list issue IDs from Section 1.2.1 that this task closes, with exact fix bullets copied from the task section.
2. **Context references:** list all required “Read context” and “Component-behaviour references” section numbers exactly as written in the task.
3. **Implementation instructions:** include step-by-step changes by file and by behavior (not abstract goals).
4. **Contract invariants:** include “must remain true” constraints while implementing.
5. **Acceptance tests:** enumerate tests with expected outputs/status.
6. **Out-of-scope list:** explicit non-goals to prevent accidental cross-task drift.
7. **Escalation list:** ambiguous areas where implementor must ask for direction before coding.

### Task 0 — Contract compatibility matrix and orchestration normalization gates
- **Read context before implementation (mandatory):** Sections 1, 3.1–3.4, 5.5, 6.1, 11.1(A/B), 11.2(B/C/D), and 12.1/12.2.
- **Component-behaviour references (Section 11):** 11.1(A, D, E), 11.2(B, C, D), 11.4(1,2,3).
- **Role:** pre-implementation guardrail to prevent interface drift and ordering bugs.
- **Issues:** 1A, 1B, 1D, 1K, 1L.
- **Desired end state:** one auditable contract matrix covering Step Functions inputs, pipeline parameters, CLI arguments, runtime config fields, and DDB contract keys for each flow.
- **Scope:**
  - define array-first ML-project contract (`ml_project_names`) and scalar normalization rules,
  - enforce validation ordering before any ML-project-specific DDB reads,
  - define strict parameter-source policy (DDB or flow payload only; no env placeholders for business parameters).
- **Checks:**
  - static contract validation for SF↔pipeline↔CLI alignment,
  - invalid ordering tests (array present, scalar absent) to prove no premature scalar key reads.
- **Completion criteria:** downstream tasks can rely on a locked interface map and ordering guarantees; task artifacts must include copied expected-behaviour bullets from referenced Section 11 components and the full mandatory task handoff bundle.

### Task 1 — Contract freeze: Batch Index + DPP + MLP + S3 schemas
- **Read context before implementation (mandatory):** Sections 1, 3 (all), 5 (all), 6.1/6.4, 11.4(1/2/4/7), and 12.4.
- **Component-behaviour references (Section 11):** 11.1(A, E), 11.3(1,2,4,9), 11.4(1,2,4,7).
- **Role:** foundational contract freeze.
- **Issues addressed:** foundational support for 1A–1L.
- **Desired end state:** authoritative code-facing contract definitions, including direct greenfield Batch Index implementation with exactly two items per batch (`<batch_id>`, `<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) and no migration phase.
- **Why current state is not desired:** fragmented path logic and inconsistent builders.
- **Files to change:**
  - shared contract/schema modules (`src/ndr/config/` or new `src/ndr/contracts/`),
  - `project_parameters_loader.py`,
  - `batch_index_loader.py`,
  - likely new `batch_index_writer.py`,
  - `output_paths.py`.
- **File-specific fixes:**
  - remove business-logic dependence on environment placeholders; business parameters must come from DDB or initiator payload,
  - explicit DPP/MLP root-prefix loaders,
  - multi-item Batch Index lookup model,
  - replace old timestamp path assumption in `output_paths.py`.
- **Required checks:** verify direct/reverse/ML-project lookup support and adequacy for all 4 flows.
- **Completion criteria:** foundational contracts/builders/loaders complete and consistent; no flow changes yet; referenced Section 11 behaviour points are explicitly satisfied and the mandatory task prompt fields are present in execution notes.

### Task 2 — Shared runtime-model refactor + pipeline-builder cleanup
- **Read context before implementation (mandatory):** Sections 1, 3.5, 5.4/5.5, 6.1/6.3, 11.2(A-E), 11.4(3/5), and 12.2.
- **Component-behaviour references (Section 11):** 11.2(A, B, C, D), 11.4(3,5).
- **Depends on:** Task 1.
- **Issues:** 1B, 1C, 1D, 1J, 1L.
- **Desired end state:** no placeholder code URI resolution, no environment-variable business parameter dependency, reduced runtime params, proper `ml_project_name` propagation, cadence-neutral naming, and Schema-C-compatible step script contracts (`code` URI + `code_metadata` hash/build identifiers).
- **Files:**
  - `src/ndr/pipeline/io_contract.py`,
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`,
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_inference.py`,
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_prediction_feature_join.py`,
  - `src/ndr/pipeline/sagemaker_pipeline_definitions_if_training.py`,
  - affected `run_*.py` wrappers.
- **Checks:**
  - no SF passes undeclared pipeline params,
  - no pipeline declares unused params,
  - all pipeline definitions smoke-build without placeholder lookup failures.
- **Completion:** builders aligned and safe for refactored SF usage, with referenced Section 11 pipeline behaviour checks passed and mandatory context bundle attached.

### Task 3 — Batch Index writer + RT prefix precomputation
- **Read context before implementation (mandatory):** Sections 1, 3.1–3.5, 5.2–5.5, 6.1/6.4, 11.1(A/E), and 12.4.
- **Component-behaviour references (Section 11):** 11.1(A), 11.3(1,3,4,5,6), 11.4(1,2,6).
- **Depends on:** Tasks 1–2.
- **Issues:** 1A, 1J + Batch Index/prefix prewrite corrections.
- **Desired end state:** RT shared writer precomputes and writes all DPP/MLP full prefixes (data/code/artifacts) and reverse-date items early from payload (`project_name`, `batch_id`, `raw_parsed_logs_s3_prefix`, `etl_ts`) plus DPP/MLP roots, including `code_metadata` hash/build fields required by Schema C.
- **Files:** new Batch Index writer module, RT SF JSON, shared runtime/prefix modules.
- **Checks:** sample-generated records complete, reverse lookup works, multi-MLP mapping correct.
- **Completion:** canonical per-batch path precompute/write method exists and matches referenced Section 11 RT + DDB behaviours and includes mandatory context bundle evidence.

### Task 4 — RT father flow refactor
- **Read context before implementation (mandatory):** Sections 1, 3.1–3.4, 5.2/5.5, 6.1, 11.1(A/B), 11.2(B/C), and 12.1/12.2.
- **Component-behaviour references (Section 11):** 11.1(A, B), 11.3(1,2,3,4,5,6), 11.4(2,5,6).
- **Depends on:** Tasks 1–3.
- **Issues:** 1A, 1B, 1D + publication branch context.
- **Desired end state:** strict payload, DPP-resolved `ml_project_names` array fan-out map, normalization/validation-before-read ordering, pre-write Batch Index, explicit branch context.
- **Files:** `docs/step_functions_jsonata/sfn_RT_features_inference_publication.json`.
- **Checks:** definition/schema validation, param validity, validation-order checks (no scalar lookup before array normalization), branch retry/failure handling, and branch iteration correctness over each array item.
- **Completion:** RT father flow structurally correct and branch-aware, with referenced Section 11(A/B) expectations met and task prompt checklist fully satisfied.

### Task 5 — Epic: Flow-1 job refactor (Delta / FG-A / PairCounts / FG-C / inference / publication)
- **Read context before implementation (mandatory):** Sections 1, 3.2/3.4, 5.2–5.5, 6.1, 11.3(1-6), and 12.2/12.3.
- **Component-behaviour references (Section 11):** 11.3(1,2,3,4,5,6), 11.4(4,5,6).
- **Depends on:** Tasks 1–4.
- **Issues:** 1D, 1E + segment-stat/path/context corrections.
- **Desired end state:** all child jobs resolve read/write/code paths from Batch Index; Pair-Counts always emits sufficient segment stats; FG-C resolves exact batch-specific prefixes from Batch Index.
- **Files:**
  - `run_delta_builder.py`, `delta_builder_job.py`,
  - `run_fg_a_builder.py`, `fg_a_builder_job.py`,
  - `run_pair_counts_builder.py`, `pair_counts_builder_job.py`,
  - `run_fg_c_builder.py`, `fg_c_builder_job.py`,
  - `run_inference_predictions.py`, `inference_predictions_job.py`,
  - `run_prediction_feature_join.py`, `prediction_feature_join_job.py`,
  - `prediction_feature_join_spec.py` (if sink types added).
- **Checks:** targeted unit tests, inter-stage contract tests, path-resolution tests from mock Batch Index.
- **Additional mandatory simplicity checks (inference/publication path):**
  1. `ml_project_name` is propagated end-to-end only where required (SF payload -> pipeline params -> CLI -> runtime config -> Batch Index branch lookup); no duplicate branch identifiers.
  2. Prediction score column contract is single-source (no hardcoded `prediction_score` mismatch across inference/join).
  3. Inference output metadata contract enforces required fields (`model_version`, `model_name`, `inference_ts`, `record_id`) with deterministic fail-fast/default policy.
  4. Prediction-feature-join consumes a deterministic snapshot contract from inference run inputs/outputs; no drift-prone re-resolution behavior.
  5. No new runtime parameters are added unless strictly required by contract freeze.
- **Additional mandatory RT feature-creation checks (Delta/FG-A/PairCounts/FG-C):**
  1. Placeholder/default contract resolution path is disabled for RT step code/data contracts.
  2. RT SF writes Batch Index records containing `batch_id` + `raw_parsed_logs_s3_prefix`; downstream reads use Batch Index pointers.
  3. Delta partition contract is validated against produced columns (`dt`,`hh`,`mm`) before write.
  4. Delta->FG-A mini-batch contract is singular and deterministic (strict mode or explicit temporary compatibility mode from DDB).
  5. FG-C fails fast (or emits explicit remediation signal) when FG-B host baselines are missing.
  6. FG-C join-key granularity checks enforce parity with FG-B host baseline grouping contract.
  7. FG-C segment fallback join rejects missing required key dimensions.
  8. PairCounts dependency placement is justified by contract or simplified to shorten RT critical path.
- **Completion:** Flow 1 fully Batch Index-driven and branch-safe, satisfying referenced Section 11 step-level expectations with mandatory context bundle included.

#### Task 5.1 — Atomic unit: Delta + FG-A contract hardening
- **Depends on:** Task 5 (epic prerequisites) and Tasks 0–4.
- **Focus:** Delta output contract, `mini_batch_id` determinism, FG-A input/output path-contract alignment.
- **Mandatory checks:** Delta partition contract tests, Delta->FG-A mini-batch contract tests, Batch Index path-resolution tests for Delta/FG-A.

#### Task 5.2 — Atomic unit: PairCounts + FG-C contract hardening
- **Depends on:** Task 5.1.
- **Focus:** PairCounts sufficiency outputs, FG-C join-key granularity and fallback rejection rules, required baseline dependency signaling.
- **Mandatory checks:** PairCounts sufficiency tests, FG-C join-key/segment fallback tests, fail-fast tests for missing required baselines.

#### Task 5.3 — Atomic unit: inference/join/publication contract alignment
- **Depends on:** Task 5.1 and Task 5.2.
- **Focus:** `ml_project_name` branch context propagation, inference metadata contract, prediction-join consumer alignment.
- **Mandatory checks:** inference->join schema/metadata contract tests, branch-context propagation tests, deterministic output path tests.

### Task 6 — Monthly machine inventory + FG-B refactor
- **Read context before implementation (mandatory):** Sections 1, 3.2/3.4/3.5.1, 5.2/5.5, 6.2, 11.1(C), 11.3(7/8), and 12.1/12.3.
- **Component-behaviour references (Section 11):** 11.1(C), 11.3(7,8), 11.4(5,6).
- **Depends on:** Tasks 1–5.
- **Issues:** 1G + `reference_month` redesign + inventory consistency.
- **Desired end state:** deterministic monthly flow, UNLOAD-only machine inventory, and canonical inventory-merge semantics that update only deltas while preserving full machine metadata/flags across months.
- **Files:**
  - `sfn_monthly_fg_b_baselines.json`,
  - `sagemaker_pipeline_definitions_unified_with_fgc.py`,
  - `run_machine_inventory_unload.py`, `machine_inventory_unload_job.py`,
  - `run_fg_b_builder.py`, `fg_b_builder_job.py`.
- **Checks:** reference-month derivation tests, unload integration tests, inventory transition tests (first-seen machine, insufficient→sufficient history, metadata/flag updates), FG-B contract tests, and verification that monthly flow does not depend on a runtime `mode` parameter in standard operation.
- **Additional mandatory simplicity checks (no over-engineering):**
  1. SF graph for monthly flow has exactly one gating dependency chain: machine-update success -> FG-B start.
  2. FG-B mapping resolution path count is minimized (one canonical source + one legacy fallback only).
  3. FG-C rejects under-specified segment fallback joins (no auto-broadening of join keys).
  4. Contract tests prove FG-C join keys match FG-B host-baseline grouping granularity.
  5. No new cross-flow runtime parameters are introduced unless required by contract freeze.
  6. Machine inventory unload uses an Option-3 flow-specific DPP query and stages unloaded rows in RAM/FS before downstream processing.
- **Completion:** monthly baseline flow deterministic and contract-safe, satisfying Section 11(C, steps 7/8) expectations and mandatory context fields.

### Task 7 — Epic: Backfill refactor
- **Read context before implementation (mandatory):** Sections 1, 3.3/3.4/3.5.1, 5.2/5.5, 6.4, 11.1(E), 11.3(9/10), and 12.1/12.4.
- **Component-behaviour references (Section 11):** 11.1(E), 11.2(E), 11.3(9), 11.4(1,6).
- **Depends on:** Tasks 1–6.
- **Issues:** 1F, 1I + Redshift fallback + reuse by all flows.
- **Desired end state:** self-detect and caller-guided modes, plus `InitialDeployment=true` cold-start bootstrap mode; ingestion-first then Redshift-UNLOAD fallback (parameters resolved from DPP in DDB), extractor-manifest-driven fanout, and Batch Index writes including Schema-C `code` + `code_metadata` fields.
- **Implementation detail requirements (mandatory, not optional):**
  1. Define explicit Batch Index write API contract (idempotency token, conditional write semantics, retry/backoff, and duplicate handling).
  2. Define exact source-to-contract field mapping from extractor/fallback outputs into Item A and Item B attributes.
  3. Define branch-level `ml_project_name` population and `s3_prefixes` materialization rules under both ingestion-path and Redshift-fallback path.
  4. Define partial-write reconciliation behavior and rollback/failure handling for split success/failure across Item A/Item B writes.
  5. Add deterministic status progression model (`planned` → `materialized` → `validated` → `published`/`failed`) with timestamped updates.
  6. Define DPP `InitialDeployment` state transition contract (`true -> false`) with strict gating rules and failure rollback (`ColdStartCompletionStatus`, `ColdStartFailureReason`).
  7. Define idempotent rerun behavior for cold-start bootstrap mode.
  8. Define family-level missing-range planner contract (`artifact_family -> [date ranges]`) and selective launch policy.
  9. Define family skip logic and dependency-safe ordering for partial recovery runs (e.g., FG-C-only gaps from training-triggered backfill).
  10. Implement Option-3 missing-raw-logs unload path with a DPP-owned custom query aligned to Delta/Pair-Counts consumers (no universal query).
  11. Implement multi-range unload support (`[{start_ts,end_ts}, ...]`) with deterministic range execution and retry semantics.
  12. Stage unloaded rows in RAM/FS of the SageMaker Processing step before downstream reconstruction and writes.
- **Files:**
  - `sfn_backfill_reprocessing.json`,
  - `sagemaker_pipeline_definitions_backfill_historical_extractor.py`,
  - `run_historical_windows_extractor.py`, `historical_windows_extractor_job.py`,
  - shared Redshift unload executor + flow-specific DPP query loaders,
  - Batch Index writer implementation used by backfill map workers.
- **Checks:** planner tests, ingestion-miss→Redshift-UNLOAD tests with DDB-resolved descriptors, manifest-to-map end-to-end contract tests, reconstructed-batch Batch Index write checks, idempotency/conditional-write tests, partial-write reconciliation tests, branch-mapping completeness tests, cold-start flag-transition gate tests, and selective family/range execution tests (full bootstrap vs targeted recovery scenarios).
- **Completion:** backfill reliable for training/FG-B/RT recovery and aligned with Section 11(E/step 9) expectations and mandatory context fields.

#### Task 7.1 — Atomic unit: planner + manifest contract freeze
- **Depends on:** Task 7 (epic prerequisites) and Tasks 0–6.
- **Focus:** family/range planner schema, selective recovery policy contract, manifest structure consumed by map execution.
- **Mandatory checks:** planner schema tests, selective family/range decision tests, contract compatibility tests with training-triggered remediation use-cases.

#### Task 7.2 — Atomic unit: extractor-manifest to map execution wiring
- **Depends on:** Task 7.1.
- **Focus:** ensure map reads extractor manifest outputs (not raw payload windows), deterministic map input shaping and branch execution behavior.
- **Mandatory checks:** extractor output -> map input E2E tests, selective execution tests, map retry/failure determinism tests.

#### Task 7.3 — Atomic unit: Redshift fallback unload execution path
- **Depends on:** Task 7.1.
- **Focus:** DPP-owned flow-specific unload query, multi-range execution, RAM/FS staging, deterministic retry semantics.
- **Mandatory checks:** ingestion-miss fallback tests, multi-range unload tests, staged-processing integrity tests.

#### Task 7.4 — Atomic unit: backfill Batch Index writes + idempotency/status
- **Depends on:** Task 7.1, 7.2, and 7.3.
- **Focus:** dual-item Batch Index writes for reconstructed batches, branch-level prefix mapping, status progression and reconciliation behavior.
- **Mandatory checks:** dual-item write tests, idempotency/conditional-write tests, partial-write reconciliation tests, status-transition contract tests.

### Task 8 — Epic: Training refactor
- **Read context before implementation (mandatory):** Sections 1, 3.2/3.5.2, 5.3/5.5, 6.3, 11.1(D), 11.2(D), 11.3(10), and 12.2/12.4.
- **Component-behaviour references (Section 11):** 11.1(D), 11.2(D), 11.3(5,6), 11.4(5,6).
- **Depends on:** Tasks 1–7.
- **Issues:** 1D, 1F, 1H, 1I, 1J, 1K, 1L + mode/evaluation/backfill dependency corrections.
- **Desired end state:** scheduled and script-triggered entry support; explicit `training_mode` (A/B/C) routing; `ml_project_name`-first evaluation gate contract with inline override support; missing-feature backfill; and contract-based path resolution (with run-varying payload fields + MLP model/evaluation lookups).
- **Lean implementation principle (mandatory):** keep runtime payload minimal; all business behavior must be read from DDB contracts (DPP/MLP/Batch Index + JobSpec), with no hard-coded business defaults, no placeholder-derived behavior, and no env-driven business fallback.

#### Task 8.1 — Canonical runtime contract (minimal trigger, DDB-owned behavior)
- **Trigger payload allowed by design (and only these fields unless explicitly approved):**
  1. `project_name`
  2. `feature_spec_version`
  3. `ml_project_name`
  4. `run_id`
  5. `execution_ts_iso`
  6. optional explicit training/evaluation bounds for ad-hoc operations (`training_start_ts`, `training_end_ts`, `eval_start_ts`, `eval_end_ts`)
- **Must be removed from runtime payload and resolved from DDB instead:**
  - `missing_windows_override`
  - `enable_history_planner`
  - `enable_auto_remediate_15m`
  - `enable_auto_remediate_fgb`
  - `enable_post_training_evaluation`
  - `enable_eval_join_publication`
  - `enable_eval_experiments_logging`
  - orchestration target names/ARNs
- **Source of truth:**
  - runtime policy and toggles from `if_training` JobSpec,
  - orchestration targets from DDB-defined orchestration target contract,
  - missing-window derivation from Batch Index + feature checks.
- **Acceptance gates:**
  1. Training SF → SageMaker pipeline input set contains only trigger-by-design fields.
  2. `run_if_training.py`/`if_training_spec.py` runtime model removes non-trigger flags.
  3. End-to-end tests show equivalent behavior controlled only by DDB updates (without changing SF payload).

#### Task 8.2 — Stage sequencing correction: verify/plan → remediate → reverify → train
- **Problem to fix:** current planner artifacts needed by remediation are generated in `train` stage, which is too late.
- **Required sequencing contract:**
  1. `verify`: detect and persist normalized missing-manifest input.
  2. `plan`: produce deterministic family/range plan from Batch Index + feature checks.
  3. `remediate`: invoke backfill/FG-B selectively by plan.
  4. `reverify`: prove required windows now available.
  5. `train`: allowed only after successful reverify gate.
- **Implementation constraint:** remediation must not infer behavior from ad-hoc payload values; only from persisted verify/plan artifacts + DDB contracts.
- **Acceptance gates:**
  1. No remediation path depends on artifacts created only in `train`.
  2. `train` hard-fails when reverify indicates unresolved required windows.
  3. Pipeline step order and state files reflect this sequence deterministically.

#### Task 8.3 — Unified missing-window schema (single contract across verify/plan/remediate)
- **Canonical schema (required):**
  - `artifact_family` (e.g., `fg_a_15m`, `fg_b_daily`)
  - `ranges`: `[{start_ts_iso,end_ts_iso}]` (disjoint and sorted)
  - `source` (`batch_index_gap`, `feature_partition_gap`, `quality_gate`)
  - `ml_project_name`
  - `project_name`, `feature_spec_version`, `run_id`
- **Rules:**
  1. Verify output must emit this schema.
  2. Remediation input must consume this exact schema.
  3. No dual semantic surfaces (`missing partition days` vs `missing 15m windows`) are allowed.
- **Acceptance gates:**
  1. Contract tests enforce schema equality for verify output and remediation input.
  2. All remediation decisions are explainable by manifest entries.

#### Task 8.4 — Selective remediation execution (no coarse global envelope)
- **Required behavior:**
  1. Backfill call uses explicit manifest ranges (or compacted disjoint ranges), not one global min/max.
  2. FG-B rebuild invocation uses precise missing reference days only.
  3. Large manifests are chunked deterministically (`chunk_index`, `chunk_size`, `chunk_hash`) with idempotent retry.
- **Forbidden behavior:**
  - collapsing all missing ranges into one giant interval,
  - truncating missing windows by retry count slice.
- **Acceptance gates:**
  1. Execution log contains 1:1 mapping between manifest chunks and remediation runs.
  2. Targeted sparse recovery does not launch unrelated families/ranges.

#### Task 8.5 — Batch Index as authoritative completeness source for training readiness
- **Required reads before remediation/train:**
  1. expected windows in `[training/eval required horizon]`,
  2. observed materialization status by family/range,
  3. unresolved windows grouped by family and `ml_project_name`.
- **Required output artifacts:**
  - `training_readiness_manifest.json`
  - `missing_windows_manifest.json`
  - `remediation_plan.json`
- **Acceptance gates:**
  1. `batch_index_table_name` is used in planner/verifier logic (not only validated/logged).
  2. Readiness artifacts include Batch Index evidence pointers (pk/sk/range selectors).

#### Task 8.6 — `ml_project_name` propagation and branch isolation (mandatory)
- **Required propagation path:**
  - training SF input → training pipeline parameters → `run_if_training.py` args → `IFTrainingRuntimeConfig` → `if_training_job.py` runtime context → downstream evaluation/remediation invocations.
- **Required downstream invocation behavior:**
  1. Evaluation-triggered inference pipeline call includes `MlProjectName`.
  2. Evaluation-triggered prediction-join pipeline call includes `MlProjectName`.
  3. Artifact/report paths and model publication metadata are branch-scoped by `ml_project_name`.
- **Acceptance gates:**
  1. No training execution path drops `ml_project_name`.
  2. Multi-ML-project concurrent training replays remain isolated in outputs and target routing.

#### Task 8.7 — Strict orchestration target resolution contract (DDB-only for business behavior)
- **Required precedence:**
  1. DDB contract value (required for enabled branch),
  2. deterministic validation (`type`, existence, permission-check readiness),
  3. fail fast with contract error code if missing/invalid.
- **Removed behavior:**
  - code-default orchestration target names as business fallback,
  - env-var fallback for business behavior.
- **Acceptance gates:**
  1. Required branches cannot start without valid DDB target entries.
  2. Readiness report records `resolution_source="ddb_contract"` for enabled branches.

#### Task 8.8 — Code URI resolution hardening (no placeholder/default-value resolution)
- **Required behavior:**
  1. Training pipeline step code location must resolve from DDB contract using concrete identity, never from `ParameterString.default_value` placeholders.
  2. Packaging/manifest metadata (`artifact_build_id`, `artifact_sha256`) must be validated at resolution time.
- **Acceptance gates:**
  1. Pipeline build tests fail if placeholder-derived URIs are detected.
  2. Contract tests enforce that every training step has resolvable concrete code URI from DDB.

#### Task 8.9 — Runtime contract hygiene for required parameters
- **Rules:**
  1. Required runtime parameters must be functionally used.
  2. Any required-but-unused runtime parameter must either be wired or removed.
  3. `MlpConfigTableName` must be used for ML-project-aware resolution or removed from required runtime list.
- **Acceptance gates:**
  1. Static checks verify every required runtime param has a concrete read/use site.
  2. Seeded `required_runtime_params` for `pipeline_if_training` match true runtime usage.

#### Task 8.10 — Implementation order inside Task 8 (mandatory)
1. Runtime contract shrink + DDB ownership (8.1, 8.9).
2. `ml_project_name` propagation and branch isolation (8.6).
3. Sequencing correction + unified manifest schema (8.2, 8.3).
4. Selective remediation mechanics (8.4).
5. Batch Index authority in readiness planner (8.5).
6. Strict DDB target resolution (8.7).
7. Code URI hardening (8.8).

#### Task 8.11 — Task-level verification bundle (must be attached before closure)
- **Contract tests:**
  - training SF input contract test (minimal trigger fields only),
  - verify→plan→remediate manifest schema compatibility tests,
  - DDB target resolution strictness tests (no fallback).
- **Orchestration tests:**
  - missing-window training-triggered backfill test (targeted sparse ranges),
  - reverify gate blocks train when unresolved windows remain.
- **ML-project tests:**
  - assert `ml_project_name` survives all training stages and downstream evaluation pipeline starts,
  - assert branch-isolated output paths and metadata.
- **Code resolution tests:**
  - fail on placeholder-based code URI resolution,
  - pass with DDB-resolved concrete step scripts + artifact metadata validation.

- **Files:**
  - `sfn_training_orchestrator.json`,
  - `sagemaker_pipeline_definitions_if_training.py`,
  - `run_if_training.py`, `if_training_spec.py`, `if_training_job.py`.
- **Checks:** mode A/B/C tests, evaluation gate tests, missing-feature backfill invocation tests, artifact/report/model path tests, minimal-runtime-contract tests, verify/plan/remediate manifest-contract tests, strict DDB target resolution tests, `ml_project_name` propagation tests, and placeholder-code-resolution guard tests.
- **Completion:** training aligned with final system model and Section 11(D/pipeline D) expected behaviour with mandatory context fields present.

### Task 9 — Hardening + final rollout validation
- **Read context before implementation (mandatory):** Sections 1, 3–6 (all), 11.1–11.4, and 12 (all gates).
- **Component-behaviour references (Section 11):** 11.1(A-E), 11.2(A-E), 11.3(1-9), 11.4(1-7).
- **Depends on:** Tasks 1–8.
- **Issues:** 1A–1L regression closure; remaining contract integrity and regression risks.
- **Desired end state:** no unjustified timestamp-centric assumptions, no placeholder runtime behavior, no env fallback for business behavior, and complete core contract tests sufficient for production hardening.
- **Files:** cross-cutting tests/validators/loaders/cleanup.
- **Checks:** unit suite, targeted job tests, contract validations, pipeline smoke-build, SF validation, fixture-driven Batch Index/DPP/MLP integrations, first-deployment cold-start E2E tests (`InitialDeployment=true` bootstrap success and flag flip to `false`), and targeted-recovery E2E tests (only missing families/ranges execute).
- **Completion:** rollout-ready and safe for production deployment; all referenced Section 11 behaviours validated via Section 12 gates and mandatory handoff bundle captured.

---

## 11. Component traceability matrix (derived from Section 6 unified target state)

This section is a traceability matrix derived from Section 6 and must remain aligned with it. It maps exact files/contracts to the same unified end-state expectations and current deviation markers.

### 11.0 Global implementation guardrails (integrated from former Section 9)
- Any task that finds prerequisite contracts not yet present must stop rather than improvise.
- Every task must add validation for contracts it introduces/depends on.
- No task should defer interface correctness to “later cleanup”.
- No task should introduce hard-coded values, environment-variable business parameters, or placeholder-only runtime assumptions; business parameters must come from DDB or flow payload.
- Backfill and RT must both write Batch Index records.
- Stable code prefixes belong in DPP/MLP; batch-varying data/artifact prefixes belong in Batch Index.
- Remove cadence-specific and product-specific naming assumptions where logic is intended to be generic.
- Source/target separation and segment-stat sufficiency are hard contracts, not best-effort behavior.

### 11.1 Step Functions (orchestration layer)

#### A) `sfn_RT_features_inference_publication.json` (RT father flow)
**Expected behaviour after plan implementation**
1. Accept payload with `project_name`, `batch_id`, `raw_parsed_logs_s3_prefix`, `etl_ts`.
2. Resolve `ml_project_names` from DPP by `project_name` lookup.
3. Normalize/validate array (`ml_project_names`) before MLP-specific operations.
4. Derive `date_partition`, `hour`, `within_hour_run_number`, `org1`, `org2`.
5. Resolve DPP/MLP roots, build all full DPP/MLP/code/artifact prefixes.
6. Write Batch Index dual items (`<batch_id>`, `<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) with full prefix map.
7. Execute per-`ml_project_name` fan-out (Map), each branch launching feature+inference+publication with explicit branch context.
8. Maintain deterministic status updates in Batch Index and lock release semantics.

**Current deviation markers**
- Current state still uses scalar-oriented `ml_project_name` handling and legacy parse surface before full normalization gates.
- Current state starts feature/inference pipelines with parameter sets that include mismatch-prone fields (`MlProjectNamesJson` etc.).
- Current Batch Index write path still follows current table shape (`pk/sk/GSI1`) rather than target dual-item schema.

#### B) `sfn_prediction_publication.json`
**Expected behaviour after plan implementation**
1. Receive explicit branch context (`project_name`, `batch_id`, `ml_project_name`).
2. Resolve publication/join prefixes from Batch Index MLP branch map keys:
   - `s3_prefixes.mlp.<ml_project_name>.predictions`
   - `s3_prefixes.mlp.<ml_project_name>.prediction_join`
   - `s3_prefixes.mlp.<ml_project_name>.publication`
3. Trigger prediction-join pipeline first, then publication pipeline with explicit dependency on join success.
4. Run prediction-join + publication in idempotent per-ML-project branch mode and emit completion/status updates with branch identity.

**Current deviation markers**
- Currently still accepts dual input styles (`ml_project_name` or `ml_project_names`) and keeps broader validation envelope than branch-context-only runtime contract.
- Uses timestamp-heavy publication identity fields from caller path instead of Batch Index-first resolution.

#### C) `sfn_monthly_fg_b_baselines.json`
**Expected behaviour after plan implementation**
1. Accept deterministic `reference_month` (`YYYY/MM`) and project identity.
2. Run UNLOAD-only machine inventory refresh with DPP-resolved Redshift descriptors.
3. Write/update Batch Index prefixes needed for monthly machine update and FG-B baseline:
   - `s3_prefixes.dpp.fg_b.machines_manifest`
   - `s3_prefixes.dpp.fg_b.machines_unload_for_update`
   - `s3_prefixes.dpp.fg_b.machines_base_stats`
   - `s3_prefixes.dpp.fg_b.segment_base_stats`
4. Run FG-B baseline build using those Batch Index/DPP/MLP-resolved paths.
5. Maintain canonical inventory merge semantics (new machine, metadata/flag transitions, history sufficiency changes).

**Current deviation markers**
- Current state still includes runtime `mode` usage in monthly orchestration and pipeline call path.
- Current code path and pipeline contracts still reflect legacy timestamp/parameter assumptions.

#### D) `sfn_training_orchestrator.json`
**Expected behaviour after plan implementation**
1. Support scheduled and script-triggered entry envelopes.
2. Require `training_mode` with values:
   - `training_and_evaluation_only`
   - `training_and_evaluation_then_prod_training`
   - `prod_training_only`
3. Accept run-varying inputs (`project_name`, `ml_project_name`, window ranges), resolve model-definition + evaluation-policy data from MLP using `ml_project_name`.
4. Resolve all IO prefixes via Batch Index + DPP/MLP.
5. Trigger backfill flow when required features are missing.

**Current deviation markers**
- Current state lacks explicit unified `training_mode` routing contract.
- Current state uses existing evaluation window/policy fields but not full `ml_project_name`-first MLP execution contract.

#### E) `sfn_backfill_reprocessing.json`
**Expected behaviour after plan implementation**
1. Support self-detect and caller-guided missing-artifact modes.
2. Consume extractor/planner output manifest directly for map fanout.
3. If ingestion partitions are missing, switch to DPP-driven Redshift UNLOAD fallback.
4. Write Batch Index records for reconstructed batches.

**Current deviation markers**
- Current map windows are still sourced from input payload fields rather than extractor manifest output contract.
- Current fallback strategy does not fully encode DPP-resolved Redshift recovery descriptors in orchestration contract.

### 11.2 SageMaker pipelines (definition layer)

#### A) Unified DPP pipeline (`sagemaker_pipeline_definitions_unified_with_fgc.py`)
**Expected behaviour after plan implementation**
- Cadence-generic naming.
- Parameters limited to contract-required runtime keys.
- Steps resolve script locations from explicit contracts (no placeholder lookups).
- Each step reads/writes by Batch Index-resolved prefixes.

**Current deviation markers**
- Current file still contains cadence-specific naming and assumptions.
- Current step code resolution still uses builder-time defaults through `resolve_step_code_uri(...)`.

#### B) Inference pipeline (`sagemaker_pipeline_definitions_inference.py`)
**Expected behaviour after plan implementation**
- Declared pipeline params must match step CLI arguments exactly.
- `ml_project_name` must be consumed by runtime CLI/config.
- No undeclared extra params from Step Functions.

**Current deviation markers**
- `MlProjectName` is declared but currently not passed into step CLI.
- Step code resolution still occurs using placeholder default values at build time.

#### C) Prediction-join pipeline (`sagemaker_pipeline_definitions_prediction_feature_join.py`)
**Expected behaviour after plan implementation**
- Branch-aware `ml_project_name` propagation required.
- Inputs/outputs resolved through Batch Index branch map.
- Pipeline parameters and CLI args strictly aligned.

**Current deviation markers**
- `MlProjectName` is declared but currently not consumed by the step CLI.
- Build-time placeholder code-resolution pattern remains.

#### D) IF training pipeline (`sagemaker_pipeline_definitions_if_training.py`)
**Expected behaviour after plan implementation**
- Accept `training_mode`, inline policy override (optional), and branch identity.
- Propagate `ml_project_name` to runtime stage handlers.
- Resolve `model_type` and evaluation metrics/thresholds from MLP policy/spec contracts by default.
- Stage chain follows mode routing and evaluation-gate policy.

**Current deviation markers**
- `MlProjectName` is declared in pipeline parameters, but current step arguments do not pass it to script runtime.
- Mode routing and policy execution remain partially represented vs. target contract.

#### E) Backfill extractor pipeline (`sagemaker_pipeline_definitions_backfill_historical_extractor.py`)
**Expected behaviour after plan implementation**
- Produce manifest entries including missing artifact families and recovery hints.
- Support DPP-driven Redshift fallback metadata in output plan.

**Current deviation markers**
- Current contracts focus on historical window extraction and do not yet fully encode artifact-family + fallback planning semantics.

### 11.3 Pipeline-step expected behaviour (post-plan)

1. **DeltaBuilderStep**
   - Must write delta outputs with `mini_batch_id`, `feature_spec_version`, and contract-safe schema fields.
   - **Current deviation:** main delta output path currently enforces schema that omits `mini_batch_id` in manifest contract.

2. **FGABuilderStep**
   - Must read exact delta prefix from Batch Index and enforce mini-batch strictness safely.
   - **Current deviation:** strict mini-batch enforcement can fail when upstream delta lacks `mini_batch_id`.

3. **PairCountsBuilderStep**
   - Must always emit sufficient segment stats and target-side coverage required downstream.
   - **Current deviation:** current implementation may still depend on existing traffic aggregation assumptions and timestamp-based output path building.

4. **FGCCorrBuilderStep**
   - Must resolve exact FG-A/FG-B/pair-context prefixes from Batch Index+DPP (no reconstructed timestamp paths).
   - **Current deviation:** currently reads configured prefixes and filters by timestamps rather than exact Batch Index path map.

5. **InferencePredictionsStep**
   - Must consume `ml_project_name` and resolve feature/prediction prefixes by Batch Index branch key.
   - Must not own cross-pipeline orchestration (ordering remains SF responsibility).
   - **Current deviation:** currently constructs paths using timestamp helper and does not pass branch identity through CLI.

6. **PredictionFeatureJoinStep**
   - Must use Batch Index for exact prediction+feature prefix resolution and route publication by branch context.
   - Must not own cross-pipeline orchestration (ordering remains SF responsibility).
   - **Current deviation:** currently path-derives via timestamp helper and does not consume `ml_project_name` in runtime CLI.

7. **MachineInventoryUnloadStep**
   - Must be UNLOAD-only and update canonical inventory deltas.
   - **Current deviation:** currently includes query+Spark fallback when UNLOAD fails.

8. **FGBaselineBuilderStep**
   - Must run monthly deterministic baseline over canonical inventory and feature inputs from Batch Index/DPP/MLP.
   - **Current deviation:** current interfaces still include legacy mode/time assumptions.

9. **HistoricalWindowsExtractorStep**
   - Must emit artifact-aware manifest consumed by backfill map and include fallback metadata.
   - **Current deviation:** current orchestration does not consume extractor output manifest as fanout source.

10. **IFTrainingEvaluationStep / training-eval handler path**
   - Must read `model_type` and evaluation metrics/thresholds from MLP-resolved contracts using `ml_project_name`, then evaluate model outputs against those thresholds/rules.
   - Must not treat Batch Index as the authority for model/evaluation policy values.
   - **Current deviation:** current contracts still partially rely on legacy payload fields and do not enforce MLP-as-authority for evaluation-policy contracts end-to-end.

### 11.4 Core code/config files behaviour matrix (in-scope)

For each file group below, expected behaviour after implementation is listed with current deviation marker.

1. `src/ndr/config/batch_index_loader.py`, `batch_index_writer.py`
   - **Expected:** dual-item (`<batch_id>` + `<YYYY/MM/dd>#<hh>#<within_hour_run_number>`) schema, no GSI dependency, full prefix-map support including per-ML-project branches.
   - **Current deviation:** current implementation still reflects legacy key/GSI-driven lookup patterns.

2. `src/ndr/config/project_parameters_loader.py`
   - **Expected:** array-first DPP linkage (`ml_project_names`), reciprocal validation per branch MLP, no env-placeholder business dependency.
   - **Current deviation:** current linkage logic is primarily single-`ml_project_name` oriented.

3. `src/ndr/pipeline/io_contract.py`
   - **Expected:** explicit deployment-time contract resolution without placeholder defaults.
   - **Current deviation:** currently called with placeholder-like builder defaults from pipeline definitions.

4. `src/ndr/processing/output_paths.py`
   - **Expected:** no cross-component authority for runtime path reconstruction; Batch Index is canonical source.
   - **Current deviation:** still used by multiple jobs for timestamp-derived path construction.

5. `src/ndr/scripts/run_*.py` wrappers (delta/fg_a/pair_counts/fg_c/inference/join/training/backfill/monthly)
   - **Expected:** reduced minimal params (`project_name`, `batch_id`, `ml_project_name` when needed), no unnecessary full timestamp parameters across components.
   - **Current deviation:** several wrappers still require or propagate full timestamps as primary routing keys.

6. `src/ndr/processing/*_job.py` runtime jobs
   - **Expected:** read/write all prefixes through Batch Index lookups; use ETL/date/hour/run attributes from Batch Index when exact timestamps are needed internally.
   - **Current deviation:** several jobs still derive paths from `batch_start_ts_iso` and related timestamp fields.

6a. Training/evaluation consumers (`run_if_training.py`, `if_training_spec.py`, `if_training_job.py`, and pipeline handlers)
   - **Expected:** resolve `model_type` + evaluation metrics/thresholds from MLP table contracts via `ml_project_name` and apply those contracts during evaluation-gate decisions.
   - **Current deviation:** evaluation inputs are still partially represented as runtime payload fields instead of fully MLP-resolved policy/spec contracts.

7. `src/ndr/scripts/create_ml_projects_parameters_table.py`
   - **Expected:** seed contracts aligned with dual-item Batch Index and array-first DPP linkage.
   - **Current deviation:** current bootstrap contracts include legacy keys/indexes and scalar linkage assumptions.

---

## 12. Implementation acceptance checklist by component category

A sub-plan implementation is accepted only if all relevant checks pass:

1. **Step Functions**
   - parameter schema validation,
   - no undeclared pipeline parameter passing,
   - explicit branch-context propagation,
   - manifest-to-map correctness (backfill),
   - deterministic failure/lock/status handling,
   - `InitialDeployment=true` routes through backfill cold-start mode before normal RT publication,
   - selective family-level launch policy enforced (only required families/ranges are triggered).

2. **Pipelines + steps**
   - no placeholder builder-time code resolution,
   - parameter declarations exactly match step CLI usage,
   - branch identity consumed where required,
   - step `code` URIs resolve to Schema-C single-file artifacts,
   - corresponding `code_metadata` hash/build fields are present for every step,
   - smoke-build success for all pipeline definitions.

3. **Jobs + wrappers**
   - Batch Index path resolution tests,
   - no forbidden timestamp-centric interface dependency,
   - contract tests on Delta→FG-A→FG-B→FG-C→inference→join chain,
   - PairCounts segment-stat sufficiency tests,
   - cold-start bootstrap gate tests (FG-B baseline availability, FG-C non-empty output, and publication readiness checks),
   - targeted-recovery tests proving missing-range-only execution (for example FG-C-only partial rebuild from training-triggered backfill).

4. **DDB contracts (Batch Index/DPP/MLP)**
   - dual-item Batch Index read/write tests,
   - Batch Index schema single-source-of-truth check (full schema defined only in Section 3.5.4; no duplicate schema bodies elsewhere),
   - `code_metadata` coverage check for every declared step code key,
   - array-first DPP linkage tests,
   - MLP branch mapping tests,
   - model-type + evaluation-policy retrieval tests from MLP for all training/evaluation consumers,
   - no env-placeholder business parameter fallback,
   - DPP cold-start control contract tests (`InitialDeployment`, completion status, failure-reason fields and state transitions).
