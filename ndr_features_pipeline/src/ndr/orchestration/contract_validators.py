"""Reusable contract validators for orchestration and interface hardening."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


TASK11_INTEGRATION_GATE_VERSION = "task11_system_readiness_gate.v1"
TASK11_CONTRACT_ERROR_CODE = "TASK11_CONTRACT_VIOLATION"
TASK11_GATE_ERROR_CODE = "TASK11_GATE_RED"
TASK12_BOOTSTRAP_GATE_VERSION = "task12_initial_deployment_bootstrap.v1"
TASK12_CONTRACT_ERROR_CODE = "TASK12_CONTRACT_VIOLATION"
TASK12_GATE_ERROR_CODE = "TASK12_BOOTSTRAP_GATE_RED"
TASK14_STARTUP_CONFORMANCE_VERSION = "task14_startup_contract_conformance.v1"
TASK14_CONTRACT_ERROR_CODE = "TASK14_CONTRACT_VIOLATION"
TASK14_GATE_ERROR_CODE = "TASK14_STARTUP_CONTRACT_RED"

TASK14_BACKFILL_SFN_TO_EXECUTOR_PRODUCER_MISMATCH = "TASK14_BACKFILL_SFN_TO_EXECUTOR_PRODUCER_MISMATCH"
TASK14_BACKFILL_SFN_TO_EXECUTOR_CONSUMER_MISMATCH = "TASK14_BACKFILL_SFN_TO_EXECUTOR_CONSUMER_MISMATCH"
TASK14_TRAINING_TO_FGB_PRODUCER_MISMATCH = "TASK14_TRAINING_TO_FGB_PRODUCER_MISMATCH"
TASK14_TRAINING_TO_FGB_CONSUMER_MISMATCH = "TASK14_TRAINING_TO_FGB_CONSUMER_MISMATCH"
TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_PRODUCER_MISMATCH = "TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_PRODUCER_MISMATCH"
TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_CONSUMER_MISMATCH = "TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_CONSUMER_MISMATCH"

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

_TASK12_REQUIRED_CHECKPOINTS = (
    "seed_machine_inventory",
    "reconstruct_historical_families",
    "build_monthly_baseline",
    "validate_readiness_manifest",
    "activate_rt_steady_state",
)

_TASK14_REQUIRED_INTERFACES = (
    "backfill_sfn_to_backfill_executor",
    "training_remediation_to_fgb_pipeline",
    "extractor_runtime_to_manifest",
)

_TASK14_INTERFACE_ERROR_CODES = {
    "backfill_sfn_to_backfill_executor": (
        TASK14_BACKFILL_SFN_TO_EXECUTOR_PRODUCER_MISMATCH,
        TASK14_BACKFILL_SFN_TO_EXECUTOR_CONSUMER_MISMATCH,
    ),
    "training_remediation_to_fgb_pipeline": (
        TASK14_TRAINING_TO_FGB_PRODUCER_MISMATCH,
        TASK14_TRAINING_TO_FGB_CONSUMER_MISMATCH,
    ),
    "extractor_runtime_to_manifest": (
        TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_PRODUCER_MISMATCH,
        TASK14_EXTRACTOR_RUNTIME_TO_MANIFEST_CONSUMER_MISMATCH,
    ),
}


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


def evaluate_task12_initial_deployment_bootstrap(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate deterministic day-0 bootstrap orchestration evidence.

    Readiness criteria must be measurable and machine-checkable. If criteria are
    missing or non-measurable, fail with explicit contract violation so operators
    stop and fix the readiness definition before rollout.
    """

    required_sections = (
        "startup_paths",
        "control_record",
        "bootstrap_manifest",
        "rt_activation",
        "recovery",
    )
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK12_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    startup_paths = evidence["startup_paths"]
    control_record = evidence["control_record"]
    bootstrap_manifest = evidence["bootstrap_manifest"]
    rt_activation = evidence["rt_activation"]
    recovery = evidence["recovery"]

    for section_name, section_value in (
        ("startup_paths", startup_paths),
        ("control_record", control_record),
        ("bootstrap_manifest", bootstrap_manifest),
        ("rt_activation", rt_activation),
        ("recovery", recovery),
    ):
        if not isinstance(section_value, Mapping):
            raise ValueError(f"{TASK12_CONTRACT_ERROR_CODE}: {section_name} must be a mapping")

    checkpoints = bootstrap_manifest.get("checkpoints")
    if not isinstance(checkpoints, Mapping):
        raise ValueError(f"{TASK12_CONTRACT_ERROR_CODE}: bootstrap_manifest.checkpoints must be a mapping")

    readiness_criteria = bootstrap_manifest.get("readiness_criteria")
    if not isinstance(readiness_criteria, Sequence) or not readiness_criteria:
        raise ValueError(
            f"{TASK12_CONTRACT_ERROR_CODE}: measurable readiness_criteria are required for bootstrap"
        )

    malformed_criteria: list[int] = []
    for idx, criterion in enumerate(readiness_criteria):
        if not isinstance(criterion, Mapping):
            malformed_criteria.append(idx)
            continue
        metric = str(criterion.get("metric") or "").strip()
        expected = criterion.get("expected")
        actual = criterion.get("actual")
        if not metric or expected is None or actual is None or "passed" not in criterion:
            malformed_criteria.append(idx)
    if malformed_criteria:
        raise ValueError(
            f"{TASK12_CONTRACT_ERROR_CODE}: readiness_criteria entries must include metric/expected/actual/passed; "
            f"malformed indexes={malformed_criteria}"
        )

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    _check(
        "12.orientation.rt_monthly_backfill_reviewed",
        bool(startup_paths.get("rt_reviewed")) and bool(startup_paths.get("monthly_reviewed")) and bool(startup_paths.get("backfill_reviewed")),
        "Startup dependency orientation completed for RT/monthly/backfill",
    )

    missing_checkpoints = [name for name in _TASK12_REQUIRED_CHECKPOINTS if name not in checkpoints]
    _check("12.checkpoints.present", not missing_checkpoints, f"Required checkpoints present (missing={missing_checkpoints})")
    _check(
        "12.checkpoints.passed",
        not missing_checkpoints and all(bool((checkpoints.get(name) or {}).get("passed")) for name in _TASK12_REQUIRED_CHECKPOINTS),
        "Required checkpoints passed deterministically",
    )

    control_status = str(control_record.get("status") or "").strip().upper()
    _check(
        "12.control.ready",
        control_status == "READY",
        f"Bootstrap control record status is READY (status={control_status or '<missing>'})",
    )
    _check(
        "12.control.deterministic_key",
        bool(str(control_record.get("control_key") or "").strip()),
        "Deterministic bootstrap control key is persisted",
    )

    _check(
        "12.readiness.criteria",
        all(bool((criterion or {}).get("passed")) for criterion in readiness_criteria),
        "Measurable readiness criteria passed",
    )

    _check(
        "12.rt.activation.contract",
        str(rt_activation.get("contract_version") or "").strip() == "bootstrap_rt_activation.v1"
        and bool(rt_activation.get("bootstrap_ready")),
        "Bootstrap outputs are authoritative RT activation inputs",
    )

    _check(
        "12.recovery.idempotent_rerun",
        bool(recovery.get("rerun_no_op_verified")),
        "Bootstrap rerun is idempotent/no-op safe",
    )
    _check(
        "12.recovery.partial_resume",
        bool(recovery.get("partial_recovery_verified")),
        "Partial bootstrap recovery is deterministic",
    )
    _check(
        "12.recovery.retry_and_rollback",
        bool(recovery.get("retry_strategy_verified")) and bool(recovery.get("rollback_strategy_verified")),
        "Retry and rollback safeguards are verified",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK12_BOOTSTRAP_GATE_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        raise ValueError(
            f"{TASK12_GATE_ERROR_CODE}: Task 12 initial deployment bootstrap gate failed checks {failed_checks}"
        )
    return report


def evaluate_task14_startup_contract_conformance(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate startup-critical producer/consumer contracts before deploy.

    Every startup-critical interface is validated in both directions:
    - producer-required fields must be consumable by consumer schema
    - consumer-required fields must be sourced by producer payload
    """

    required_sections = ("interfaces", "release_gate", "safeguards")
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK14_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    interfaces = evidence["interfaces"]
    release_gate = evidence["release_gate"]
    safeguards = evidence["safeguards"]

    if not isinstance(interfaces, Sequence):
        raise ValueError(f"{TASK14_CONTRACT_ERROR_CODE}: interfaces must be a sequence")
    if not isinstance(release_gate, Mapping):
        raise ValueError(f"{TASK14_CONTRACT_ERROR_CODE}: release_gate must be a mapping")
    if not isinstance(safeguards, Mapping):
        raise ValueError(f"{TASK14_CONTRACT_ERROR_CODE}: safeguards must be a mapping")

    by_interface_id: dict[str, Mapping[str, Any]] = {}
    for item in interfaces:
        if not isinstance(item, Mapping):
            raise ValueError(f"{TASK14_CONTRACT_ERROR_CODE}: each interfaces entry must be a mapping")
        interface_id = str(item.get("interface_id") or "").strip()
        if not interface_id:
            raise ValueError(f"{TASK14_CONTRACT_ERROR_CODE}: interface_id is required for every interface")
        by_interface_id[interface_id] = item

    missing_interfaces = [iid for iid in _TASK14_REQUIRED_INTERFACES if iid not in by_interface_id]
    if missing_interfaces:
        raise ValueError(
            f"{TASK14_CONTRACT_ERROR_CODE}: missing startup interfaces {missing_interfaces}"
        )

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    def _validate_interface(interface_id: str) -> None:
        interface = by_interface_id[interface_id]
        producer_fields = {str(v).strip() for v in (interface.get("producer_fields") or []) if str(v).strip()}
        consumer_fields = {str(v).strip() for v in (interface.get("consumer_fields") or []) if str(v).strip()}
        producer_to_consumer = interface.get("producer_to_consumer_map") or {}
        consumer_to_producer = interface.get("consumer_to_producer_map") or {}

        if not isinstance(producer_to_consumer, Mapping) or not isinstance(consumer_to_producer, Mapping):
            raise ValueError(
                f"{TASK14_CONTRACT_ERROR_CODE}: {interface_id} maps must be mapping values"
            )

        missing_on_consumer = sorted(producer_fields - consumer_fields)
        missing_on_producer = sorted(consumer_fields - producer_fields)
        producer_code, consumer_code = _TASK14_INTERFACE_ERROR_CODES[interface_id]
        _check(
            f"14.contract.{interface_id}.producer_to_consumer",
            not missing_on_consumer,
            f"{producer_code}: producer fields not accepted by consumer={missing_on_consumer}",
        )
        _check(
            f"14.contract.{interface_id}.consumer_to_producer",
            not missing_on_producer,
            f"{consumer_code}: consumer-required fields missing from producer={missing_on_producer}",
        )

        producer_map_keys = {str(k).strip() for k in producer_to_consumer}
        producer_map_targets = {str(v).strip() for v in producer_to_consumer.values()}
        consumer_map_keys = {str(k).strip() for k in consumer_to_producer}
        consumer_map_targets = {str(v).strip() for v in consumer_to_producer.values()}

        _check(
            f"14.mapping.{interface_id}.producer_keys",
            producer_map_keys == producer_fields,
            f"{producer_code}: producer map keys must match producer_fields (missing={sorted(producer_fields - producer_map_keys)}, extra={sorted(producer_map_keys - producer_fields)})",
        )
        _check(
            f"14.mapping.{interface_id}.producer_targets",
            producer_map_targets <= consumer_fields,
            f"{producer_code}: producer map targets must resolve to consumer_fields (unknown={sorted(producer_map_targets - consumer_fields)})",
        )
        _check(
            f"14.mapping.{interface_id}.consumer_keys",
            consumer_map_keys == consumer_fields,
            f"{consumer_code}: consumer map keys must match consumer_fields (missing={sorted(consumer_fields - consumer_map_keys)}, extra={sorted(consumer_map_keys - consumer_fields)})",
        )
        _check(
            f"14.mapping.{interface_id}.consumer_targets",
            consumer_map_targets <= producer_fields,
            f"{consumer_code}: consumer map targets must resolve to producer_fields (unknown={sorted(consumer_map_targets - producer_fields)})",
        )

    for interface_id in _TASK14_REQUIRED_INTERFACES:
        _validate_interface(interface_id)

    _check(
        "14.release.block_on_red",
        bool(release_gate.get("block_on_red")),
        "TASK14_RELEASE_GATE_POLICY_MISSING: release gate must block deploy on red startup contract status",
    )
    _check(
        "14.release.status_green",
        str(release_gate.get("status") or "").strip().lower() == "green",
        "TASK14_RELEASE_GATE_RED: startup contract matrix status must be green",
    )
    _check(
        "14.safeguards.idempotency",
        bool(safeguards.get("idempotency_verified")),
        "TASK14_IDEMPOTENCY_GUARD_MISSING: startup retries require deterministic idempotency validation",
    )
    _check(
        "14.safeguards.retry",
        bool(safeguards.get("retry_verified")),
        "TASK14_RETRY_GUARD_MISSING: startup retries must be contract-safe",
    )
    _check(
        "14.safeguards.rollback",
        bool(safeguards.get("rollback_verified")),
        "TASK14_ROLLBACK_GUARD_MISSING: startup rollback strategy must be validated",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK14_STARTUP_CONFORMANCE_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        failed_details = [check["detail"] for check in checks if not check["passed"]]
        raise ValueError(
            f"{TASK14_GATE_ERROR_CODE}: Task 14 startup contract conformance gate failed checks "
            f"{failed_checks}; diagnostics={failed_details}"
        )
    return report
