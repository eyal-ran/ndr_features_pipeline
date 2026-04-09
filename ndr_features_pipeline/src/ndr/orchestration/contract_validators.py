"""Reusable contract validators for orchestration and interface hardening."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


TASK11_INTEGRATION_GATE_VERSION = "task11_system_readiness_gate.v1"
TASK11_CONTRACT_ERROR_CODE = "TASK11_CONTRACT_VIOLATION"
TASK11_GATE_ERROR_CODE = "TASK11_GATE_RED"

_TASK11_REQUIRED_FINDINGS = (
    "F1.1",
    "F1.2",
    "F2.1",
    "F2.2",
    "F2.3",
    "F3.1",
    "F3.2",
    "F3.3",
    "F4.1",
    "F4.2",
    "F4.3",
    "F4.4",
    "F4.5",
    "F5.1",
)

_TASK11_REQUIRED_METRICS = (
    "fallback_frequency",
    "backfill_latency_seconds",
    "unresolved_missing_ranges_count",
)

_TASK11_REQUIRED_ALARMS = (
    "fallback_frequency_alarm",
    "backfill_latency_alarm",
    "unresolved_missing_ranges_alarm",
)


def normalize_ml_project_names(
    ml_project_names: Sequence[str] | None,
    ml_project_name: str | None = None,
) -> list[str]:
    """Normalize to array-first ml_project_names and enforce deterministic ordering."""
    normalized: list[str] = []
    if ml_project_names:
        for value in ml_project_names:
            candidate = str(value).strip()
            if not candidate:
                raise ValueError("ml_project_names cannot contain empty values")
            if candidate not in normalized:
                normalized.append(candidate)
    elif ml_project_name:
        candidate = str(ml_project_name).strip()
        if not candidate:
            raise ValueError("ml_project_name cannot be blank when provided")
        normalized = [candidate]

    if not normalized:
        raise ValueError("ml_project_names must resolve to a non-empty list")
    return normalized


def validate_pipeline_parameter_alignment(
    *,
    declared_parameters: Iterable[str],
    passed_parameters: Iterable[str],
) -> None:
    """Fail fast when SF->pipeline interfaces drift."""
    declared = set(declared_parameters)
    passed = set(passed_parameters)
    undeclared = sorted(passed - declared)
    if undeclared:
        raise ValueError(f"Pipeline parameters passed but undeclared: {undeclared}")
    missing = sorted(declared - passed)
    if missing:
        raise ValueError(f"Pipeline parameters declared but not passed: {missing}")


_FORBIDDEN_BUSINESS_FALLBACK_MARKERS = (
    "<required:",
    "<placeholder",
    "${",
    "env_fallback",
    "code_default",
)


def validate_no_business_fallback_markers(*, values: Mapping[str, Any], context: str) -> None:
    """Reject runtime/business contract values that still contain fallback placeholders.

    Task-9 hardening requires concrete DDB-resolved values for orchestration ownership.
    This validator intentionally checks for marker substrings rather than exact values so
    contract checks fail fast even when payload shapes evolve.
    """

    offending: list[str] = []
    for key, value in values.items():
        text = str(value).strip()
        if not text:
            offending.append(f"{key}=<blank>")
            continue
        lowered = text.lower()
        if any(marker in lowered for marker in _FORBIDDEN_BUSINESS_FALLBACK_MARKERS):
            offending.append(f"{key}={text}")
    if offending:
        raise ValueError(
            f"{context}: fallback/placeholder business markers are not allowed: {offending}"
        )


def validate_targeted_recovery_manifest(
    *,
    manifest_entries: Sequence[Mapping[str, Any]],
    requested_families: Sequence[str],
) -> None:
    """Ensure targeted-recovery execution remains selective and deterministic."""

    requested = {str(f).strip() for f in requested_families if str(f).strip()}
    if not requested:
        raise ValueError("requested_families must contain at least one artifact family")

    if not manifest_entries:
        raise ValueError("manifest_entries cannot be empty for targeted recovery")

    planned: set[str] = set()
    for idx, entry in enumerate(manifest_entries):
        family = str(entry.get("artifact_family") or "").strip()
        ranges = entry.get("ranges")
        if not family:
            raise ValueError(f"manifest_entries[{idx}] is missing artifact_family")
        if family not in requested:
            raise ValueError(
                f"manifest_entries[{idx}] requests unsupported family '{family}' "
                f"(requested_families={sorted(requested)})"
            )
        if not isinstance(ranges, list) or not ranges:
            raise ValueError(f"manifest_entries[{idx}] for family '{family}' must include non-empty ranges")
        planned.add(family)

    missing = sorted(requested - planned)
    if missing:
        raise ValueError(f"targeted recovery manifest is missing requested families: {missing}")


def evaluate_task11_system_readiness_gate(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate Task 11 integration/rollout evidence and return go/no-go decision.

    Contract goals:
    - green gates for RT, monthly, training, backfill interactions
    - replay/idempotency + retry safety validated
    - rollback dry run validated
    - producer/consumer contract drift eliminated
    - observability metrics/alarms present and green
    - integrated closure for findings F1.1–F5.1
    """

    required_sections = (
        "flows",
        "replay",
        "rollback",
        "producer_consumer",
        "observability",
        "finding_closure",
    )
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(
            f"{TASK11_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}"
        )

    flows = evidence["flows"]
    replay = evidence["replay"]
    rollback = evidence["rollback"]
    producer_consumer = evidence["producer_consumer"]
    observability = evidence["observability"]
    finding_closure = evidence["finding_closure"]

    if not isinstance(flows, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: flows must be a mapping")
    if not isinstance(replay, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: replay must be a mapping")
    if not isinstance(rollback, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: rollback must be a mapping")
    if not isinstance(producer_consumer, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: producer_consumer must be a mapping")
    if not isinstance(observability, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: observability must be a mapping")
    if not isinstance(finding_closure, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: finding_closure must be a mapping")

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    _check("11.flow.rt", bool(flows.get("rt_backfill_green")), "RT↔Backfill integration gate")
    _check(
        "11.flow.monthly",
        bool(flows.get("monthly_backfill_green")),
        "Monthly↔Backfill integration gate",
    )
    _check(
        "11.flow.training",
        bool(flows.get("training_backfill_green")),
        "Training↔Backfill integration gate",
    )
    _check("11.flow.backfill", bool(flows.get("backfill_interactions_green")), "Backfill interaction gate")

    _check("11.replay.idempotency", bool(replay.get("idempotency_verified")), "Replay idempotency validation")
    _check("11.replay.retry", bool(replay.get("retry_verified")), "Retry-safety validation")

    _check("11.rollback", str(rollback.get("status") or "").strip().lower() == "passed", "Rollback dry run")

    _check(
        "11.contract.producer_consumer",
        bool(producer_consumer.get("interfaces_verified")) and not bool(producer_consumer.get("drift_detected")),
        "Producer/consumer interface verification and drift check",
    )

    observed_metrics = {str(name).strip() for name in (observability.get("metrics") or []) if str(name).strip()}
    observed_alarms = {str(name).strip() for name in (observability.get("alarms") or []) if str(name).strip()}
    signal_checks = observability.get("signal_checks") or {}
    if not isinstance(signal_checks, Mapping):
        raise ValueError(f"{TASK11_CONTRACT_ERROR_CODE}: observability.signal_checks must be a mapping")

    missing_metrics = sorted(set(_TASK11_REQUIRED_METRICS) - observed_metrics)
    missing_alarms = sorted(set(_TASK11_REQUIRED_ALARMS) - observed_alarms)

    _check("11.obs.metrics", not missing_metrics, f"Required metrics present (missing={missing_metrics})")
    _check("11.obs.alarms", not missing_alarms, f"Required alarms present (missing={missing_alarms})")
    _check(
        "11.obs.signals",
        all(bool(signal_checks.get(signal_name)) for signal_name in _TASK11_REQUIRED_METRICS),
        "Monitoring signals validated for required metrics",
    )

    missing_findings = [fid for fid in _TASK11_REQUIRED_FINDINGS if fid not in finding_closure]
    if missing_findings:
        raise ValueError(
            f"{TASK11_CONTRACT_ERROR_CODE}: finding_closure missing required findings {missing_findings}"
        )
    for finding_id in _TASK11_REQUIRED_FINDINGS:
        _check(
            f"11.finding.{finding_id}",
            bool(finding_closure.get(finding_id)),
            f"Integrated finding {finding_id} is closed",
        )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK11_INTEGRATION_GATE_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }

    if failed_checks:
        raise ValueError(
            f"{TASK11_GATE_ERROR_CODE}: Task 11 system readiness gate failed checks {failed_checks}"
        )

    return report
