# Task 15 — Startup failure classes and remediation paths orientation

This orientation is required before implementing startup observability artifacts.

## Failure classes (consumer-aligned)
1. **missing_range_remediation_gap (IDP-2)**
   - Consumer signal: startup readiness remains `false` because required ranges are still missing.
   - Producer signal: remediation requests were invoked but unresolved ranges remain above zero.
2. **extractor_bootstrap_fragility (IDP-5)**
   - Consumer signal: startup output manifests are incomplete due to extractor mode instability.
   - Producer signal: fallback-source usage spikes while bootstrap family reconstruction is incomplete.
3. **raw_log_fallback_not_integrated (IDP-7)**
   - Consumer signal: no deterministic fallback artifacts reach downstream flow even when logs exist.
   - Producer signal: fallback mode selected but recovery outputs absent.
4. **startup_contract_validation_failure (Task 14 dependency)**
   - Consumer signal: startup-critical contracts fail and release gate blocks deployment.
   - Producer signal: explicit Task 14 mismatch diagnostics emitted.

## Remediation paths
- **RT path:** keep inference/publication gated until bootstrap readiness is green.
- **Monthly path:** rerun FG-B baseline materialization only after missing-range count returns to zero.
- **Backfill path:** targeted missing-range replay with deterministic idempotency keying.
- **Training path:** invoke training remediation only when verification manifests indicate missing dependencies.

## Escalation ownership model
- Primary owner team: **NDR Platform**.
- Escalation service: **NDR-Startup-PagerDuty**.
- Sev1: customer-facing readiness outage or data-correctness risk.
- Sev2: startup degradation with bounded impact and active workaround.
- Sev3: warning-level drift/noise with no active customer impact.

If this ownership model is absent, implementation should fail fast with `TASK15_OWNERSHIP_MODEL_UNDEFINED`.
