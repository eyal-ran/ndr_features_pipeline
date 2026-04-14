## Part A — Repo-verified algorithm and fix specification

## Scope and assumptions

- Current build/validate/smoke scripts are print-only stubs and must become executable artifact lifecycle producers/consumers.
- Step runtime contracts are DDB-driven (`scripts.steps.<Step>.code_prefix_s3`, `entry_script`, and artifact metadata).
- Artifact-mode runtime expects deterministic resolution and execution behavior across all pipeline families (15m streaming, 15m dependent, FG-B baseline, machine inventory unload, inference, prediction join, IF training, backfill).
- Deployment notebook content must remain minimal in logic changes, but should be improved in operator clarity and execution safety.

---

## 1) Core script fixes (artifact lifecycle scripts)

### 1.1 `src/ndr/scripts/run_code_bundle_build.py`

#### Current behavior (faulty)
- Parses args and only prints `build_started ...`.
- Does not build archives, compute hashes, upload to S3, or emit manifest metadata.

#### Correct behavior (replacement algorithm / pseudo-code)

```python
args = parse_args(
    --project-name,
    --feature-spec-version,
    --artifact-build-id,
    --region-name (optional),
    --dpp-config-table-name (optional),
    --workspace-root (default cwd),
    --manifest-out (default /tmp/code_bundle_manifest.json),
    --artifact-format (tar.gz only in v1),
)

validate_non_placeholder_inputs(args)

# Discover all active pipeline step contracts from DDB for this project/spec
step_contracts = discover_all_active_step_contracts(
    project_name=args.project_name,
    feature_spec_version=args.feature_spec_version,
    dpp_config_table_name=args.dpp_config_table_name,
)

# Build deterministic source staging tree
stage = mkdtemp()
copy_tree("src/ndr", f"{stage}/src/ndr")
copy_required_repo_files(stage)  # e.g. pyproject/setup metadata if needed
ensure_all_entry_scripts_present(step_contracts, stage)

# Deterministic manifest
manifest = build_manifest(
    project_name=args.project_name,
    feature_spec_version=args.feature_spec_version,
    artifact_build_id=args.artifact_build_id,
    files=hash_all_files(stage),
    steps=step_contracts,
)
write_json(manifest, f"{stage}/MANIFEST.json")

# Deterministic tarball creation
archive_local = "/tmp/source.tar.gz"
create_deterministic_tar_gz(stage, archive_local)
artifact_sha256 = sha256_file(archive_local)

# Upload same immutable bytes to each step target URI
# target: <code_prefix_s3>/artifacts/<artifact_build_id>/source.tar.gz
step_outputs = []
for c in step_contracts:
    uri = f"{c.code_prefix_s3.rstrip('/')}/artifacts/{args.artifact_build_id}/source.tar.gz"
    s3_put_file(archive_local, uri)
    s3_put_text(artifact_sha256, uri.replace("source.tar.gz", "source.tar.gz.sha256"))
    s3_put_json(manifest, uri.replace("source.tar.gz", "manifest.json"))
    step_outputs.append({
        "pipeline_job_name": c.pipeline_job_name,
        "step_name": c.step_name,
        "entry_script": c.entry_script,
        "code_artifact_s3_uri": uri,
        "artifact_build_id": args.artifact_build_id,
        "artifact_sha256": artifact_sha256,
        "artifact_format": "tar.gz",
    })

out = {
    "project_name": args.project_name,
    "feature_spec_version": args.feature_spec_version,
    "artifact_build_id": args.artifact_build_id,
    "artifact_sha256": artifact_sha256,
    "artifact_format": "tar.gz",
    "step_artifacts": step_outputs,
}
write_json(out, args.manifest_out)
print_json(out)
```

#### Critical implementation notes
- `entry_script` path must be valid relative to extraction root expected by runtime launch (`python <entry_script>`).
- No optional alternate layout for entry scripts unless launch args and contract value are updated together.

---

### 1.2 `src/ndr/scripts/run_code_artifact_validate.py`

#### Current behavior (faulty)
- Parses args and only prints `validation_started ...`.
- Does not validate object existence, hash integrity, archive structure, or entry script presence.

#### Correct behavior (replacement algorithm / pseudo-code)

```python
args = parse_args(
    --project-name,
    --feature-spec-version,
    --artifact-build-id,
    --build-manifest-in,  # output from build step
    --validation-report-out (default /tmp/code_artifact_validate_report.json),
)

build_output = read_json(args.build_manifest_in)
assert build_output["artifact_build_id"] == args.artifact_build_id

results = []
for item in build_output["step_artifacts"]:
    uri = item["code_artifact_s3_uri"]
    expected_sha = item["artifact_sha256"]
    expected_format = item["artifact_format"]
    entry_script = item["entry_script"]

    head = s3_head(uri)
    assert head.exists and head.content_length > 0

    local = s3_download(uri)
    observed_sha = sha256_file(local)
    assert observed_sha == expected_sha

    assert_archive_format(local, expected_format)
    assert_archive_contains(local, entry_script)

    results.append({"step_name": item["step_name"], "status": "PASS"})

report = {
    "artifact_build_id": args.artifact_build_id,
    "status": "PASS",
    "validated_steps": len(results),
    "step_results": results,
}
write_json(report, args.validation_report_out)
print_json(report)
```

---

### 1.3 `src/ndr/scripts/run_code_smoke_validate.py`

#### Current behavior (faulty)
- Parses args and only prints `smoke_started ...`.
- Does not verify runtime executability or import readiness from artifact contents.

#### Correct behavior (replacement algorithm / pseudo-code)

```python
args = parse_args(
    --project-name,
    --feature-spec-version,
    --artifact-build-id,
    --build-manifest-in,
    --validate-all-steps (default true),
    --smoke-report-out (default /tmp/code_smoke_validate_report.json),
)

build_output = read_json(args.build_manifest_in)
step_artifacts = build_output["step_artifacts"]

smoke_results = []
for item in step_artifacts:
    local = s3_download(item["code_artifact_s3_uri"])
    extract_dir = extract_archive(local)

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{extract_dir}/src:" + env.get("PYTHONPATH", "")

    # mimic artifact-mode entry execution shape
    r = run_subprocess(["python", item["entry_script"], "--help"], cwd=extract_dir, env=env, timeout=120)

    if r.failed:
        # fallback diagnostics
        run_subprocess(["python", "-m", "py_compile", item["entry_script"]], cwd=extract_dir, env=env)
        run_subprocess(["python", "-c", "import ndr; print('ok')"], cwd=extract_dir, env=env)
        smoke_results.append({"step_name": item["step_name"], "status": "FAIL", "reason": r.stderr})
    else:
        smoke_results.append({"step_name": item["step_name"], "status": "PASS"})

if any(x["status"] == "FAIL" for x in smoke_results):
    final = "FAIL"
    exit_code = 2
else:
    final = "PASS"
    exit_code = 0

report = {
    "artifact_build_id": args.artifact_build_id,
    "status": final,
    "step_results": smoke_results,
}
write_json(report, args.smoke_report_out)
print_json(report)
exit(exit_code)
```

---

## 2) Additional code/config files requiring explicit fixes

This section addresses the requested follow-up: for each additional file, exactly what is faulty now, how it currently operates, and what should replace it.

### 2.1 `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`

#### Faulty current operation
- Uses `resolve_step_code_uri(...)` for `code=...`, but still runs static `python -m ndr.scripts...` command shapes.
- Does not consistently use artifact-mode aware launch argument construction.

#### Required replacement behavior
- For each step (`RTRawInputResolverStep`, `DeltaBuilderStep`, `FGABuilderStep`, `PairCountsBuilderStep`, `FGCCorrBuilderStep`, `FGBaselineBuilderStep`, `MachineInventoryUnloadStep`):
  1. Resolve full contract via `resolve_step_execution_contract(...)`.
  2. Set `code=contract.script_s3_uri`.
  3. Build args via `build_processing_step_launch_args(entry_script=contract.entry_script, module_name=<current module>, artifact_uri=contract.code_artifact_s3_uri, passthrough_args=[...])`.
  4. Keep step dependency graph unchanged.

#### Replacement pseudo-code template (per step)

```python
contract = resolve_step_execution_contract(..., step_name="DeltaBuilderStep")
step = ProcessingStep(
    name="DeltaBuilderStep",
    processor=processor,
    code=contract.script_s3_uri,
    job_arguments=build_processing_step_launch_args(
        entry_script=contract.entry_script,
        module_name="ndr.scripts.run_delta_builder",
        artifact_uri=contract.code_artifact_s3_uri,
        passthrough_args=[...existing args...],
    ),
    inputs=[],
    outputs=[],
)
```

---

### 2.2 `src/ndr/pipeline/sagemaker_pipeline_definitions_code_bundle_build.py`
### 2.3 `src/ndr/pipeline/sagemaker_pipeline_definitions_code_artifact_validate.py`
### 2.4 `src/ndr/pipeline/sagemaker_pipeline_definitions_code_smoke_validate.py`

#### Faulty current operation
- Use placeholder script code URIs (`s3://placeholder/...`).
- No formal artifact handoff between bundle -> validate -> smoke steps (manifest/report flow not wired).

#### Required replacement behavior
- Replace placeholder `code` values with concrete script locations produced by deployment promotion or packaged in pipeline source.
- Wire outputs/inputs so:
  - build step writes build manifest,
  - validate step consumes build manifest and writes validation report,
  - smoke step consumes build manifest + validation report.
- Fail pipeline if validation/smoke report status is not PASS.

#### Replacement pseudo-code skeleton

```python
build_step = ProcessingStep(..., outputs=[ProcessingOutput(source="/tmp/out", destination="s3://.../build/")])
validate_step = ProcessingStep(..., inputs=[ProcessingInput(source=build_step.properties.ProcessingOutputConfig...)] )
smoke_step = ProcessingStep(..., inputs=[ProcessingInput(source=build_step...), ProcessingInput(source=validate_step...)])
validate_step.add_depends_on([build_step])
smoke_step.add_depends_on([validate_step])
```

---

### 2.5 `src/ndr/pipeline/io_contract.py` (targeted hardening)

#### Current behavior
- Artifact mode launch args are available, but enforcement depends on dual-read mode flag.

#### Required fixes
- Keep backward compatibility, but add explicit helper for “strict artifact-mode required” checks usable by deployment smoke path.
- Add guard for `entry_script` path traversal (`..`, absolute paths) before launch arg construction.

#### Replacement pseudo-code

```python
def validate_entry_script_path(entry_script: str) -> None:
    if entry_script.startswith("/") or ".." in PurePosixPath(entry_script).parts:
        raise ValueError("invalid entry_script path")

# call before return in resolve_step_script_contract / build launch args
```

---

### 2.6 `src/ndr/scripts/create_ml_projects_parameters_table.py` (contract discovery helper)

#### Current behavior
- Seeds include required step contract fields, but artifact lifecycle scripts currently have no canonical discovery helper.

#### Required fixes
- Add reusable helper (or adjacent utility module) that enumerates active pipeline job names and step contracts for a project/spec.
- Ensure code bundle build uses this helper (no hardcoded partial list drift).

#### Replacement pseudo-code

```python
def discover_all_active_step_contracts(project_name, feature_spec_version, table_name=None):
    # query dpp_config items where job_name_version begins_with("pipeline_") + feature_spec_version
    # parse scripts.steps map
    # return sorted list of contracts
    ...
```

---

### 2.7 Deployment notebooks (minimum logic changes, mandatory documentation-cell fix)

Files:
- `src/ndr/deployment/canonical_end_to_end_deployment_plan.ipynb`
- `src/ndr/deployment/canonical_end_to_end_deployment_plan.md` (mirror)

#### Mandatory fix required by this request
- Add a **markdown cell before every code cell** that explains:
  - behavior,
  - purpose,
  - required inputs,
  - expected outputs,
  - expected result,
  - usage instructions with examples.

#### Additional minimum alignment fixes
- Keep code logic changes minimal, but align artifact sections with corrected lifecycle:
  - explicitly describe that Cell 13 commands are operational steps that must emit manifest/report artifacts,
  - document that placeholders are invalid for production execution,
  - add short operator checklist for artifact pointers promotion readiness.

#### Notebook markdown insertion pseudo-code (automation approach)

```python
nb = load_notebook("canonical_end_to_end_deployment_plan.ipynb")
new_cells = []
for cell in nb.cells:
    if cell.cell_type == "code":
        md = build_explanatory_markdown_for(cell.source)
        new_cells.append(markdown_cell(md))
    new_cells.append(cell)
nb.cells = new_cells
save_notebook(nb)

# Regenerate markdown mirror from notebook or maintain exact synchronized mirror edit policy.
```

#### Template for each inserted markdown cell

```markdown
### Cell <N> explanation
- **Purpose:** ...
- **What this code does:** ...
- **Required inputs:** ...
- **Expected outputs:** ...
- **Expected result/state change:** ...
- **How to run safely:** ...
- **Example:** ...
```

---

## 3) Repository verification conclusion

If only the original script-level pseudo-code is implemented, end-to-end artifact-mode consumption is **not guaranteed**. The additional fixes above (especially unified pipeline definition migration and deployment pipeline definition wiring) are required to satisfy the runnable, all-steps consumption objective.

---

## 4) Design answer to dependency-path concern

- Automatic per-file dependency inference can be attempted but is fragile for dynamic imports/runtime behavior.
- The robust design is deterministic full-runtime packaging (`src/ndr` + all declared `entry_script` files), strict path validation, and contract-driven launch behavior.
- Contract fields (`entry_script`, `code_artifact_s3_uri`, `artifact_*`) remain the source of truth; no hidden fallback or implicit path guessing.

---

## Part B — Ordered implementation tasks (detailed prompts)

> **Mandatory instruction block (must be included and enforced in EACH task):**
> - Implement producer and consumer changes together for each contract touched.
> - Prefer the simplest design that preserves deterministic behavior and system coherence.
> - Enforce DDB-first business configuration and minimal runtime payloads.
> - Fail fast with explicit error codes/messages for contract violations.
> - Include idempotency, retry, and rollback considerations.
> - Conclude each task by updating all impacted project documentation files.
> - If code execution components are added, implement them as SageMaker ProcessingStep Python/PySpark paths.

---

## Task 0 — Freeze artifact lifecycle contracts and acceptance criteria

### Mandatory reading
- `docs/archive/debug_records/code_bundle_artifact_algorithm_plan.md` (Part A + Part B).
- `src/ndr/pipeline/io_contract.py`.
- `src/ndr/scripts/create_ml_projects_parameters_table.py`.
- `src/ndr/contracts.py`.

### Finding/issue (detailed)
Artifact lifecycle responsibilities and metadata shape are distributed and partially implicit.

### Fix objective
Create one explicit machine-validated contract baseline for build output manifest, validation report, and smoke report.

### Detailed build instructions
1. Define JSON schemas for:
   - `code_bundle_build_output.v1`,
   - `code_artifact_validate_report.v1`,
   - `code_smoke_validate_report.v1`.
2. Define and document explicit error codes for common failures (missing object/hash mismatch/archive invalid/entry missing/smoke failure).
3. Add schema validators and tests.

### Validation/tests
- Schema validation tests (pass/fail).
- Missing-field/unknown-field tests.

### Deliverables
- Schemas, validators, tests, documented error taxonomy.

### Completion criteria
No implementation proceeds with ambiguous artifact metadata/report structures.

### Mandatory general instructions (required in this task)
- Implement producer and consumer changes together.
- Keep deterministic behavior.
- No placeholder field without behavioral meaning.
- Idempotent/retry-safe/rollback-aware behavior.

---

## Task 1 — Implement deterministic code bundle build script

### Mandatory reading
- `src/ndr/scripts/run_code_bundle_build.py`.
- `src/ndr/scripts/create_ml_projects_parameters_table.py`.
- `src/ndr/pipeline/io_contract.py`.

### Finding/issue (detailed)
Build script is non-functional and does not produce runnable artifacts.

### Fix objective
Produce deterministic, hashed, uploaded artifact bundles and machine-readable build output manifest.

### Detailed build instructions
1. Implement contract discovery helper usage (no hardcoded partial pipeline list drift).
2. Implement deterministic archive creation (`source.tar.gz`).
3. Upload to `<code_prefix_s3>/artifacts/<artifact_build_id>/source.tar.gz` for all discovered steps.
4. Emit sidecars and build output manifest.
5. Add explicit placeholder/value validation.

### Validation/tests
- Unit tests for deterministic tar/hash.
- Mocked S3 upload tests.
- Contract discovery coverage tests.

### Deliverables
- Functional build script + tests + docs.

### Completion criteria
Build script produces deterministic artifacts + manifest that downstream steps can consume.

### Mandatory general instructions (required in this task)
- Producer/consumer alignment.
- Deterministic failure modes and explicit error codes.
- Idempotent uploads and safe retries.

---

## Task 2 — Implement artifact validation script with strict integrity checks

### Mandatory reading
- `src/ndr/scripts/run_code_artifact_validate.py`.
- `src/ndr/pipeline/io_contract.py`.

### Finding/issue (detailed)
Validation script currently performs no integrity checks.

### Fix objective
Guarantee S3 object existence, hash correctness, archive validity, and entry script existence.

### Detailed build instructions
1. Read build output manifest.
2. For each step artifact: HEAD object, download, hash compare, archive format check, entry presence check.
3. Emit validation report artifact.
4. Fail non-retriably on contract/hash violations.

### Validation/tests
- Positive/negative unit tests for each validation stage.

### Deliverables
- Functional validation script + report schema compliance tests.

### Completion criteria
Validation script deterministically passes only correct artifacts.

### Mandatory general instructions (required in this task)
- Deterministic errors and auditable reports.
- Idempotent and retry-safe I/O behavior.

### Task 2 implementation status
- `run_code_artifact_validate.py` now enforces deterministic artifact integrity checks against the frozen build manifest contract:
  - validates manifest schema before any S3 I/O,
  - performs per-step S3 `HEAD` + download + SHA-256 verification,
  - verifies `tar.gz` archive readability and declared `entry_script` presence,
  - emits `code_artifact_validate_report.v1` and fails fast on first failed step.
- Failure taxonomy aligns with Task 0 report schema error codes:
  - `CODE_ARTIFACT_OBJECT_MISSING`,
  - `CODE_ARTIFACT_DOWNLOAD_FAILED`,
  - `CODE_ARTIFACT_HASH_MISMATCH`,
  - `CODE_ARTIFACT_ARCHIVE_INVALID`,
  - `CODE_ARTIFACT_ENTRY_SCRIPT_MISSING`,
  - `CODE_ARTIFACT_SCHEMA_INVALID` (top-level contract/argument mismatch path).

---

## Task 3 — Implement smoke validation script for runnable entrypoints

### Mandatory reading
- `src/ndr/scripts/run_code_smoke_validate.py`.
- `src/ndr/pipeline/io_contract.py`.

### Finding/issue (detailed)
No current proof that artifact contents can actually execute in expected runtime shape.

### Fix objective
Demonstrate executable entry scripts and import readiness from packaged artifacts.

### Detailed build instructions
1. Consume build output manifest.
2. Download/extract each artifact.
3. Run `python <entry_script> --help` in artifact-like environment.
4. Capture and emit smoke report.
5. Fail pipeline on any step smoke failure.

### Validation/tests
- Smoke test harness unit tests (pass/fail cases).
- Path-handling and env setup tests.

### Deliverables
- Functional smoke script + tests + docs.

### Completion criteria
Smoke script proves artifacts are runnable for all included step contracts.

### Mandatory general instructions (required in this task)
- Deterministic, auditable outputs.
- No hidden fallback semantics.

---

## Task 4 — Migrate unified_with_fgc pipeline definitions to contract-based artifact launch

### Mandatory reading
- `src/ndr/pipeline/sagemaker_pipeline_definitions_unified_with_fgc.py`.
- `src/ndr/pipeline/io_contract.py`.

### Finding/issue (detailed)
Unified pipelines use code URI resolution but static module launch args, producing mixed semantics.

### Fix objective
Unify launch behavior with contract-aware artifact/module dual mode.

### Detailed build instructions
1. Replace `resolve_step_code_uri` usage with `resolve_step_execution_contract` per step.
2. Replace fixed `python -m ...` args with `build_processing_step_launch_args(...)`.
3. Preserve existing runtime parameters and step dependencies.
4. Add regression tests for all unified steps contract resolution.

### Validation/tests
- Contract resolution tests per unified step.
- Launch-arg shape tests in artifact and dual-read modes.

### Deliverables
- Updated unified pipeline defs + tests.

### Completion criteria
All unified steps support deterministic artifact-mode consumption.

### Mandatory general instructions (required in this task)
- Preserve backward compatibility where explicitly required.
- Ensure deterministic behavior in both artifact and dual-read transitional modes.

---

## Task 5 — Fix deployment code pipeline definitions (bundle/validate/smoke wiring)

### Mandatory reading
- `src/ndr/pipeline/sagemaker_pipeline_definitions_code_bundle_build.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_code_artifact_validate.py`.
- `src/ndr/pipeline/sagemaker_pipeline_definitions_code_smoke_validate.py`.

### Finding/issue (detailed)
Placeholder script locations and missing explicit handoff artifacts prevent reliable orchestration.

### Fix objective
Make deployment code pipelines fully runnable and data-linked.

### Detailed build instructions
1. Replace placeholder `code=...` script URIs with deployable script paths.
2. Add `ProcessingOutput` / `ProcessingInput` handoff artifacts between steps.
3. Enforce step ordering and fail-fast on non-PASS reports.
4. Add contract tests for pipeline definitions.

### Validation/tests
- Pipeline-definition unit tests.
- Handoff contract tests (manifest/report paths).

### Deliverables
- Corrected pipeline definitions + tests.

### Completion criteria
Deployment code pipelines are runnable without manual path patching.

### Mandatory general instructions (required in this task)
- Deterministic producer/consumer artifact contracts.
- Explicit fail-fast semantics.

---

## Task 6 — Notebook alignment with minimal code changes + mandatory explanatory markdown cells

### Mandatory reading
- `src/ndr/deployment/canonical_end_to_end_deployment_plan.ipynb`.
- `src/ndr/deployment/canonical_end_to_end_deployment_plan.md`.

### Finding/issue (detailed)
Notebook operator guidance is insufficiently explicit for safe execution; mandatory explanatory markdown before every code cell is missing.

### Fix objective
Align notebook docs with artifact lifecycle plan while minimizing logic changes.

### Detailed build instructions
1. Insert one markdown cell before **every** code cell in `.ipynb`.
2. Each inserted markdown cell must include:
   - behavior,
   - purpose,
   - required input,
   - expected output,
   - expected result,
   - usage instructions,
   - runnable example.
3. Mirror all notebook changes in `.md` mirror file.
4. Keep code changes minimal; update only where necessary for new artifact flow clarity.

### Validation/tests
- Notebook structure checker:
  - every code cell has immediately preceding markdown cell,
  - required explanatory headings present.
- Mirror parity check (`.ipynb` <-> `.md`).

### Deliverables
- Updated notebook and markdown mirror + structure validation script/tests.

### Completion criteria
Notebook is operator-safe, fully explanatory, and aligned with artifact lifecycle implementation.

### Mandatory general instructions (required in this task)
- Documentation and code parity required.
- No hidden operational assumptions.

---

## Task 7 — End-to-end artifact lifecycle integration and release gate

### Mandatory reading
- All files touched by Tasks 0–6.

### Finding/issue (detailed)
Individual fixes are insufficient unless validated as a coherent full workflow.

### Fix objective
Prove end-to-end: build -> validate -> smoke -> promoted contracts -> runtime consumption across all step families.

### Detailed build instructions
1. Add integration tests for full lifecycle on representative project/spec.
2. Validate all families are covered (streaming/dependent/FG-B/unload/inference/join/training/backfill).
3. Add release-gate checks:
   - contract schema checks,
   - pipeline definition checks,
   - notebook structure checks,
   - smoke success threshold.
4. Define rollback playbook for failed promotions.

### Validation/tests
- Full targeted pytest matrix.
- Contract drift and integration flow tests.

### Deliverables
- Integration tests, gate scripts, release checklist.

### Completion criteria
System is release-ready with deterministic, auditable artifact-mode behavior for every pipeline step.

### Mandatory general instructions (required in this task)
- No release without end-to-end green gate.
- Full documentation update required.

---

## Part B matrix — Task-to-fix traceability

| Fix area | T0 | T1 | T2 | T3 | T4 | T5 | T6 | T7 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Contract schemas + error taxonomy | ✅ | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ✅ |
| Deterministic bundle creation + S3 URI correctness | ◻️ | ✅ | ◻️ | ◻️ | ◻️ | ✅ | ◻️ | ✅ |
| Artifact hash/format/entry validation | ◻️ | ◻️ | ✅ | ◻️ | ◻️ | ✅ | ◻️ | ✅ |
| Smoke executability guarantees | ◻️ | ◻️ | ◻️ | ✅ | ✅ | ✅ | ◻️ | ✅ |
| Unified pipeline contract launch migration | ◻️ | ◻️ | ◻️ | ◻️ | ✅ | ◻️ | ◻️ | ✅ |
| Code build/validate/smoke pipeline wiring | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ✅ | ◻️ | ✅ |
| Notebook mandatory markdown-before-code policy | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ✅ | ✅ |
| Full end-to-end release gate and rollback readiness | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ◻️ | ✅ |

Legend: ✅ implemented in task scope, ◻️ not primary scope.

---

## Execution order
1. Task 0
2. Task 1
3. Task 2
4. Task 3
5. Task 4
6. Task 5
7. Task 6
8. Task 7

Rationale: freeze contracts first, then implement producer/validator/smoke primitives, then migrate consumers and orchestration wiring, then align notebooks, then run final release gate.

---

## Non-negotiable acceptance bar
- No placeholder runtime field that does not drive behavior.
- No consumer field without producer.
- No producer field without consumer (unless explicitly validated metadata no-op).
- Contract violations fail fast with explicit error code and message.
- Readiness and smoke decisions are deterministic and auditable.
- All touched paths are idempotent, retry-safe, rollback-aware.
- Every pipeline family demonstrates artifact-mode consumption readiness via tests.
