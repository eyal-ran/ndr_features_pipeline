# Task 7 code-artifact lifecycle release checklist

## Lifecycle gate preconditions
- [ ] `code_bundle_build_output.v1`, `code_artifact_validate_report.v1`, and `code_smoke_validate_report.v1` schema validators are present in release image.
- [ ] Release gate configuration loaded from `docs/release/task7_code_artifact_release_gate_config.json`.
- [ ] Evidence file prepared at `docs/archive/debug_records/task7_code_artifact_lifecycle_gate_evidence.json` (or release candidate equivalent).

## Targeted integration matrix (required families)
- [ ] 15m streaming family validated end-to-end (build -> validate -> smoke -> runtime consumption).
- [ ] 15m dependent family validated end-to-end.
- [ ] FG-B baseline family validated end-to-end.
- [ ] Machine inventory unload family validated end-to-end.
- [ ] Inference family validated end-to-end.
- [ ] Prediction join family validated end-to-end.
- [ ] IF training family validated end-to-end.
- [ ] Backfill family validated end-to-end.

## Deterministic release gates
- [ ] Contract drift check (`check_contract_drift_v3.py`) passed.
- [ ] Pipeline definition checks passed for all touched pipeline families.
- [ ] Notebook structure check passed (`markdown-before-code` requirement).
- [ ] Notebook parity check passed (`.ipynb` and `.md` mirror sync).
- [ ] Retry/replay re-run produced deterministic PASS decisions and stable report outputs.

## Producer/consumer alignment and runtime consumption
- [ ] Each family has explicit producer/consumer handoff evidence.
- [ ] No handoff depends on implicit/default runtime behavior.
- [ ] Runtime step consumption verified for all covered families.

## Rollback readiness (mandatory)
- [ ] Failed validation rollback path tested and evidence linked.
- [ ] Failed smoke rollback path tested and evidence linked.
- [ ] Deterministic replay of rollback actions verified.
- [ ] Rollback playbook reviewed and operator-approved.

## No-go conditions
- Any required family missing from targeted matrix.
- Any lifecycle stage (`build/validate/smoke`) status not `PASS`.
- Contract drift, pipeline definition checks, notebook checks, or producer/consumer alignment unresolved.
- Rollback tests missing for failed validation or failed smoke paths.
