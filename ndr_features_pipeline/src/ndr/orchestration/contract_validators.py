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

TASK15_STARTUP_OBSERVABILITY_VERSION = "task15_initial_deployment_observability.v1"
TASK15_CONTRACT_ERROR_CODE = "TASK15_CONTRACT_VIOLATION"
TASK15_GATE_ERROR_CODE = "TASK15_OBSERVABILITY_GATE_RED"
TASK9_RELEASE_HARDENING_VERSION = "task9_release_hardening_gate.v1"
TASK9_CONTRACT_ERROR_CODE = "TASK9_CONTRACT_VIOLATION"
TASK9_GATE_ERROR_CODE = "TASK9_RELEASE_GATE_RED"
TASK7_FINAL_RELEASE_GATE_VERSION = "task7_v3_final_release_gate.v1"
TASK7_CONTRACT_ERROR_CODE = "TASK7_CONTRACT_VIOLATION"
TASK7_GATE_ERROR_CODE = "TASK7_FINAL_RELEASE_GATE_RED"
TASK7_ARTIFACT_LIFECYCLE_GATE_VERSION = "task7_code_artifact_lifecycle_gate.v1"
TASK7_ARTIFACT_CONTRACT_ERROR_CODE = "TASK7_ARTIFACT_CONTRACT_VIOLATION"
TASK7_ARTIFACT_GATE_ERROR_CODE = "TASK7_ARTIFACT_RELEASE_GATE_RED"
TASK7_ARTIFACT_ROLLBACK_ERROR_CODE = "TASK7_ARTIFACT_ROLLBACK_REQUIRED"
TASK6_CROSS_FLOW_CONFORMANCE_VERSION = "task6_cross_flow_contract_conformance.v1"
TASK6_CONTRACT_ERROR_CODE = "TASK6_CONTRACT_VIOLATION"
TASK6_GATE_ERROR_CODE = "TASK6_CROSS_FLOW_CONTRACT_RED"

_TASK15_REQUIRED_METRICS = (
    "bootstrap_duration_seconds",
    "startup_remediation_invocation_count",
    "startup_unresolved_missing_range_count",
    "startup_fallback_source_mode_count",
    "startup_contract_validation_failure_count",
)

_TASK15_REQUIRED_FAILURE_CLASSES = (
    "missing_range_remediation_gap",
    "extractor_bootstrap_fragility",
    "raw_log_fallback_not_integrated",
    "startup_contract_validation_failure",
)

_TASK15_ALLOWED_SEVERITIES = ("sev1", "sev2", "sev3")
_TASK9_REQUIRED_SCENARIOS = (
    "normal",
    "missing_dependency",
    "fallback",
    "duplicate_replay",
    "partial_failure_retry",
)
_TASK9_REQUIRED_FLOWS = ("monthly", "rt", "backfill", "training", "control_plane")
_TASK6_REQUIRED_FLOWS = ("monthly", "rt", "backfill", "bootstrap", "training", "deployment")
_TASK7_REQUIRED_CRITICAL_CHECKS = (
    "bootstrap_jsonata_querylanguage_declared",
    "readiness_from_input_antipattern_blocked",
    "backfill_requested_families_honored",
    "rt_raw_fallback_contract_enforced",
)
_TASK7_REQUIRED_STARTUP_GATES = (
    "startup_contract_gate",
    "startup_observability_gate",
    "deployment_precondition_gate",
)
_TASK7_REQUIRED_FLOWS = ("monthly", "rt", "backfill", "bootstrap", "training", "deployment")
_TASK7_ARTIFACT_REQUIRED_FAMILIES = (
    "streaming",
    "dependent",
    "fg_b_baseline",
    "unload",
    "inference",
    "join",
    "training",
    "backfill",
)

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

_TASK6_INTERFACE_ERROR_CODES = {
    "monthly_readiness_artifact": (
        "TASK6_MONTHLY_READINESS_PRODUCER_MISMATCH",
        "TASK6_MONTHLY_READINESS_CONSUMER_MISMATCH",
    ),
    "rt_readiness_artifact": (
        "TASK6_RT_READINESS_PRODUCER_MISMATCH",
        "TASK6_RT_READINESS_CONSUMER_MISMATCH",
    ),
    "rt_raw_input_resolution": (
        "TASK6_RT_RAW_INPUT_PRODUCER_MISMATCH",
        "TASK6_RT_RAW_INPUT_CONSUMER_MISMATCH",
    ),
    "backfill_request_v2": (
        "TASK6_BACKFILL_REQUEST_PRODUCER_MISMATCH",
        "TASK6_BACKFILL_REQUEST_CONSUMER_MISMATCH",
    ),
    "backfill_execution_request_v2": (
        "TASK6_BACKFILL_EXECUTION_PRODUCER_MISMATCH",
        "TASK6_BACKFILL_EXECUTION_CONSUMER_MISMATCH",
    ),
    "bootstrap_jsonata_semantics": (
        "TASK6_BOOTSTRAP_QUERYLANGUAGE_PRODUCER_MISMATCH",
        "TASK6_BOOTSTRAP_QUERYLANGUAGE_CONSUMER_MISMATCH",
    ),
    "training_remediation_request": (
        "TASK6_TRAINING_REMEDIATION_PRODUCER_MISMATCH",
        "TASK6_TRAINING_REMEDIATION_CONSUMER_MISMATCH",
    ),
    "step_code_artifact_contract": (
        "TASK6_STEP_CODE_ARTIFACT_PRODUCER_MISMATCH",
        "TASK6_STEP_CODE_ARTIFACT_CONSUMER_MISMATCH",
    ),
}

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


def evaluate_task6_cross_flow_contract_conformance(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate Task-6 producer/consumer alignment across touched v3 flows."""

    required_sections = ("flows", "interfaces", "strict_controls", "safeguards")
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    flows = {str(v).strip() for v in (evidence.get("flows") or []) if str(v).strip()}
    interfaces = evidence.get("interfaces") or []
    strict_controls = evidence.get("strict_controls") or {}
    safeguards = evidence.get("safeguards") or {}

    if not isinstance(interfaces, Sequence):
        raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: interfaces must be a sequence")
    if not isinstance(strict_controls, Mapping):
        raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: strict_controls must be a mapping")
    if not isinstance(safeguards, Mapping):
        raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: safeguards must be a mapping")

    by_interface_id: dict[str, Mapping[str, Any]] = {}
    for item in interfaces:
        if not isinstance(item, Mapping):
            raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: each interfaces entry must be a mapping")
        interface_id = str(item.get("interface_id") or "").strip()
        if not interface_id:
            raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: interface_id is required for every interface")
        by_interface_id[interface_id] = item

    required_interface_ids = set(_TASK6_INTERFACE_ERROR_CODES)
    missing_interfaces = sorted(required_interface_ids - set(by_interface_id))
    if missing_interfaces:
        raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: missing required interfaces {missing_interfaces}")

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    _check(
        "6.flows.coverage",
        set(_TASK6_REQUIRED_FLOWS) <= flows,
        f"Full-system flow coverage must include {_TASK6_REQUIRED_FLOWS}",
    )

    def _validate_interface(interface_id: str) -> None:
        interface = by_interface_id[interface_id]
        producer_fields = {str(v).strip() for v in (interface.get("producer_fields") or []) if str(v).strip()}
        consumer_fields = {str(v).strip() for v in (interface.get("consumer_fields") or []) if str(v).strip()}
        producer_to_consumer = interface.get("producer_to_consumer_map") or {}
        consumer_to_producer = interface.get("consumer_to_producer_map") or {}
        metadata_noop_fields = {
            str(v).strip() for v in (interface.get("metadata_noop_fields") or []) if str(v).strip()
        }

        if not isinstance(producer_to_consumer, Mapping) or not isinstance(consumer_to_producer, Mapping):
            raise ValueError(f"{TASK6_CONTRACT_ERROR_CODE}: {interface_id} maps must be mapping values")

        missing_on_consumer = sorted(producer_fields - consumer_fields - metadata_noop_fields)
        missing_on_producer = sorted(consumer_fields - producer_fields - metadata_noop_fields)
        producer_code, consumer_code = _TASK6_INTERFACE_ERROR_CODES[interface_id]
        _check(
            f"6.contract.{interface_id}.producer_to_consumer",
            not missing_on_consumer,
            f"{producer_code}: producer fields not accepted by consumer={missing_on_consumer}",
        )
        _check(
            f"6.contract.{interface_id}.consumer_to_producer",
            not missing_on_producer,
            f"{consumer_code}: consumer-required fields missing from producer={missing_on_producer}",
        )

        producer_map_keys = {str(k).strip() for k in producer_to_consumer}
        producer_map_targets = {str(v).strip() for v in producer_to_consumer.values()}
        consumer_map_keys = {str(k).strip() for k in consumer_to_producer}
        consumer_map_targets = {str(v).strip() for v in consumer_to_producer.values()}

        _check(
            f"6.mapping.{interface_id}.producer_keys",
            producer_map_keys == producer_fields,
            f"{producer_code}: producer map keys must match producer_fields "
            f"(missing={sorted(producer_fields - producer_map_keys)}, extra={sorted(producer_map_keys - producer_fields)})",
        )
        _check(
            f"6.mapping.{interface_id}.producer_targets",
            producer_map_targets <= (consumer_fields | metadata_noop_fields),
            f"{producer_code}: producer map targets must resolve to consumer_fields or metadata_noop_fields "
            f"(unknown={sorted(producer_map_targets - (consumer_fields | metadata_noop_fields))})",
        )
        _check(
            f"6.mapping.{interface_id}.consumer_keys",
            consumer_map_keys == consumer_fields,
            f"{consumer_code}: consumer map keys must match consumer_fields "
            f"(missing={sorted(consumer_fields - consumer_map_keys)}, extra={sorted(consumer_map_keys - consumer_fields)})",
        )
        _check(
            f"6.mapping.{interface_id}.consumer_targets",
            consumer_map_targets <= (producer_fields | metadata_noop_fields),
            f"{consumer_code}: consumer map targets must resolve to producer_fields or metadata_noop_fields "
            f"(unknown={sorted(consumer_map_targets - (producer_fields | metadata_noop_fields))})",
        )

    for interface_id in sorted(required_interface_ids):
        _validate_interface(interface_id)

    _check(
        "6.strict.unknown_fields",
        bool(strict_controls.get("reject_unknown_fields")),
        "TASK6_UNKNOWN_FIELDS_NOT_REJECTED: unknown/undeclared fields must fail fast",
    )
    _check(
        "6.strict.missing_required_fields",
        bool(strict_controls.get("reject_missing_required_fields")),
        "TASK6_MISSING_REQUIRED_FIELDS_NOT_REJECTED: missing required fields must fail fast",
    )
    _check(
        "6.strict.version_mismatch",
        bool(strict_controls.get("reject_contract_version_mismatch")),
        "TASK6_CONTRACT_VERSION_MISMATCH_NOT_REJECTED: contract version mismatches must fail fast",
    )
    _check(
        "6.safeguards.idempotency",
        bool(safeguards.get("idempotency_verified")),
        "TASK6_IDEMPOTENCY_GUARD_MISSING: idempotency checks must be green",
    )
    _check(
        "6.safeguards.retry",
        bool(safeguards.get("retry_verified")),
        "TASK6_RETRY_GUARD_MISSING: retry checks must be green",
    )
    _check(
        "6.safeguards.rollback",
        bool(safeguards.get("rollback_verified")),
        "TASK6_ROLLBACK_GUARD_MISSING: rollback checks must be green",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK6_CROSS_FLOW_CONFORMANCE_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        failed_details = [check["detail"] for check in checks if not check["passed"]]
        raise ValueError(
            f"{TASK6_GATE_ERROR_CODE}: Task 6 cross-flow contract conformance failed checks "
            f"{failed_checks}; diagnostics={failed_details}"
        )
    return report


def evaluate_task15_initial_deployment_observability(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate startup observability/rollback package readiness for production rollout."""

    required_sections = ("orientation", "observability", "rollback", "validation")
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    orientation = evidence["orientation"]
    observability = evidence["observability"]
    rollback = evidence["rollback"]
    validation = evidence["validation"]

    for section_name, section_value in (
        ("orientation", orientation),
        ("observability", observability),
        ("rollback", rollback),
        ("validation", validation),
    ):
        if not isinstance(section_value, Mapping):
            raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: {section_name} must be a mapping")

    startup_paths = {str(v).strip() for v in (orientation.get("startup_paths") or []) if str(v).strip()}
    reviewed_failures = bool(orientation.get("startup_failure_classes_reviewed"))
    reviewed_remediation = bool(orientation.get("remediation_paths_reviewed"))

    metrics = {str(v).strip() for v in (observability.get("metrics") or []) if str(v).strip()}
    alarms = observability.get("alarms") or []
    dashboard = observability.get("dashboard") or {}
    runbooks = observability.get("runbooks") or {}
    incident_model = observability.get("incident_model") or {}

    if not isinstance(alarms, Sequence):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: observability.alarms must be a sequence")
    if not isinstance(dashboard, Mapping):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: observability.dashboard must be a mapping")
    if not isinstance(runbooks, Mapping):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: observability.runbooks must be a mapping")
    if not isinstance(incident_model, Mapping):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: observability.incident_model must be a mapping")

    ownership_defined = bool(str(incident_model.get("primary_owner_team") or "").strip()) and bool(
        str(incident_model.get("escalation_policy") or "").strip()
    )
    if not ownership_defined:
        raise ValueError(
            "TASK15_OWNERSHIP_MODEL_UNDEFINED: incident ownership/escalation model is required before implementation"
        )

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    _check(
        "15.orientation.startup_paths",
        startup_paths >= {"rt", "monthly", "backfill", "training"},
        f"Startup orientation covers RT/monthly/backfill/training (observed={sorted(startup_paths)})",
    )
    _check("15.orientation.failure_classes", reviewed_failures, "Startup failure classes reviewed")
    _check("15.orientation.remediation_paths", reviewed_remediation, "Startup remediation paths reviewed")

    missing_metrics = sorted(set(_TASK15_REQUIRED_METRICS) - metrics)
    _check("15.observability.metrics", not missing_metrics, f"Required startup metrics emitted (missing={missing_metrics})")

    alarm_records: list[Mapping[str, Any]] = []
    for idx, alarm in enumerate(alarms):
        if not isinstance(alarm, Mapping):
            raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: alarms[{idx}] must be a mapping")
        alarm_records.append(alarm)

    covered_failure_classes = {
        str(record.get("failure_class") or "").strip() for record in alarm_records if str(record.get("failure_class") or "").strip()
    }
    severities = {str(record.get("severity") or "").strip().lower() for record in alarm_records if str(record.get("severity") or "").strip()}
    noisy_alarms = [
        str(record.get("alarm_name") or f"alarms[{idx}]")
        for idx, record in enumerate(alarm_records)
        if int(record.get("noise_budget_max_alerts_per_day") or 0) <= 0
    ]
    alarms_missing_runbooks = [
        str(record.get("alarm_name") or f"alarms[{idx}]")
        for idx, record in enumerate(alarm_records)
        if not str(record.get("runbook_id") or "").strip() or str(record.get("runbook_id") or "").strip() not in runbooks
    ]

    _check(
        "15.observability.failure_class_alarms",
        covered_failure_classes >= set(_TASK15_REQUIRED_FAILURE_CLASSES),
        f"Failure classes have actionable alarms (missing={sorted(set(_TASK15_REQUIRED_FAILURE_CLASSES) - covered_failure_classes)})",
    )
    _check(
        "15.observability.severity_mapping",
        severities <= set(_TASK15_ALLOWED_SEVERITIES) and {"sev1", "sev2"} <= severities,
        f"Alarm severities follow production mapping (observed={sorted(severities)})",
    )
    _check("15.observability.noise_budget", not noisy_alarms, f"Noise budgets configured for all alarms (invalid={noisy_alarms})")
    _check(
        "15.observability.runbooks",
        not alarms_missing_runbooks and all(bool((runbooks.get(k) or {}).get("owner_team")) for k in runbooks),
        f"Every alarm references an owned runbook (missing={alarms_missing_runbooks})",
    )

    dashboard_panels = {str(v).strip() for v in (dashboard.get("panels") or []) if str(v).strip()}
    _check(
        "15.observability.dashboard",
        set(_TASK15_REQUIRED_METRICS) <= dashboard_panels,
        f"Dashboard exposes startup metrics used by alarms/readiness (missing={sorted(set(_TASK15_REQUIRED_METRICS)-dashboard_panels)})",
    )

    rollback_switch = rollback.get("switch") or {}
    rollback_safety = rollback.get("safety") or {}
    if not isinstance(rollback_switch, Mapping):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: rollback.switch must be a mapping")
    if not isinstance(rollback_safety, Mapping):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: rollback.safety must be a mapping")

    _check(
        "15.rollback.switch.ddb_first",
        str(rollback_switch.get("config_source") or "").strip().lower() == "dynamodb"
        and bool(str(rollback_switch.get("control_key") or "").strip()),
        "Rollback switch is DDB-first with deterministic control key",
    )
    _check(
        "15.rollback.safety",
        bool(rollback_safety.get("idempotency_verified"))
        and bool(rollback_safety.get("retry_verified"))
        and bool(rollback_safety.get("rollback_verified")),
        "Rollback switch safety checks include idempotency/retry/rollback",
    )

    synthetic_failures = validation.get("synthetic_failures") or []
    rollback_drill = validation.get("rollback_drill") or {}
    if not isinstance(synthetic_failures, Sequence):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: validation.synthetic_failures must be a sequence")
    if not isinstance(rollback_drill, Mapping):
        raise ValueError(f"{TASK15_CONTRACT_ERROR_CODE}: validation.rollback_drill must be a mapping")

    triggered_classes = {
        str(item.get("failure_class") or "").strip()
        for item in synthetic_failures
        if isinstance(item, Mapping) and str(item.get("failure_class") or "").strip()
    }
    synthetic_alarm_failures = [
        str(item.get("failure_class") or "<missing>")
        for item in synthetic_failures
        if isinstance(item, Mapping) and not bool(item.get("alarm_triggered"))
    ]

    _check(
        "15.validation.synthetic",
        triggered_classes >= set(_TASK15_REQUIRED_FAILURE_CLASSES) and not synthetic_alarm_failures,
        f"Synthetic startup failures trigger expected alarms (missing_classes={sorted(set(_TASK15_REQUIRED_FAILURE_CLASSES)-triggered_classes)}, alarm_failures={synthetic_alarm_failures})",
    )
    _check(
        "15.validation.rollback_drill",
        bool(rollback_drill.get("executed"))
        and bool(rollback_drill.get("restored_stable_state"))
        and bool(rollback_drill.get("no_data_corruption")),
        "Rollback drill restores stable state without data corruption",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK15_STARTUP_OBSERVABILITY_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        raise ValueError(
            f"{TASK15_GATE_ERROR_CODE}: Task 15 startup observability gate failed checks {failed_checks}"
        )
    return report


def evaluate_task9_release_hardening_gate(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate Task 9 release gate readiness from end-to-end scenario evidence."""

    required_sections = ("scenario_matrix", "release_gate", "producer_consumer", "rollback")
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    scenarios = evidence["scenario_matrix"]
    release_gate = evidence["release_gate"]
    producer_consumer = evidence["producer_consumer"]
    rollback = evidence["rollback"]

    if not isinstance(scenarios, Sequence):
        raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: scenario_matrix must be a sequence")
    if not isinstance(release_gate, Mapping):
        raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: release_gate must be a mapping")
    if not isinstance(producer_consumer, Mapping):
        raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: producer_consumer must be a mapping")
    if not isinstance(rollback, Mapping):
        raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: rollback must be a mapping")

    scenario_rows: dict[str, Mapping[str, Any]] = {}
    for idx, scenario in enumerate(scenarios):
        if not isinstance(scenario, Mapping):
            raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: scenario_matrix[{idx}] must be a mapping")
        scenario_id = str(scenario.get("scenario_id") or "").strip()
        if not scenario_id:
            raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: scenario_matrix[{idx}] missing scenario_id")
        scenario_rows[scenario_id] = scenario

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    missing_scenarios = sorted(set(_TASK9_REQUIRED_SCENARIOS) - set(scenario_rows))
    _check(
        "9.matrix.required_scenarios",
        not missing_scenarios,
        f"TASK9_SCENARIO_MATRIX_INCOMPLETE: missing required scenarios={missing_scenarios}",
    )

    critical_scenarios = []
    passed_critical = 0
    for scenario_id, scenario in scenario_rows.items():
        status = str(scenario.get("status") or "").strip().lower()
        is_critical = bool(scenario.get("critical"))
        deterministic = bool(scenario.get("deterministic"))
        failure_handling = bool(scenario.get("failure_handling_validated"))
        standalone = bool(scenario.get("standalone_passed"))
        integration = bool(scenario.get("integration_passed"))
        flow_coverage = {str(v).strip() for v in (scenario.get("flows_covered") or []) if str(v).strip()}

        _check(
            f"9.matrix.{scenario_id}.status",
            status == "passed",
            f"TASK9_SCENARIO_FAILED: scenario={scenario_id} status={status or '<missing>'}",
        )
        _check(
            f"9.matrix.{scenario_id}.deterministic",
            deterministic,
            f"TASK9_NON_DETERMINISTIC_BEHAVIOR: scenario={scenario_id} must be deterministic",
        )
        _check(
            f"9.matrix.{scenario_id}.failure_handling",
            failure_handling,
            f"TASK9_FAILURE_HANDLING_GAP: scenario={scenario_id} must validate retries/rollback handling",
        )
        _check(
            f"9.matrix.{scenario_id}.standalone_and_integration",
            standalone and integration,
            f"TASK9_EXECUTION_SCOPE_GAP: scenario={scenario_id} must pass standalone and integrated execution",
        )
        _check(
            f"9.matrix.{scenario_id}.flow_coverage",
            flow_coverage >= set(_TASK9_REQUIRED_FLOWS),
            f"TASK9_FLOW_COVERAGE_GAP: scenario={scenario_id} missing flows={sorted(set(_TASK9_REQUIRED_FLOWS)-flow_coverage)}",
        )

        if is_critical:
            critical_scenarios.append(scenario_id)
            if status == "passed":
                passed_critical += 1

    critical_threshold = float(release_gate.get("critical_pass_threshold") or 1.0)
    critical_rate = 1.0 if not critical_scenarios else passed_critical / len(critical_scenarios)
    _check(
        "9.release.critical_threshold",
        critical_rate >= critical_threshold,
        f"TASK9_CRITICAL_SCENARIO_THRESHOLD_FAILED: pass_rate={critical_rate:.3f} threshold={critical_threshold:.3f}",
    )
    _check(
        "9.release.block_on_fail",
        bool(release_gate.get("block_release_on_critical_failure")),
        "TASK9_RELEASE_POLICY_INVALID: block_release_on_critical_failure must be true",
    )

    contract_edges = producer_consumer.get("contract_edges") or []
    if not isinstance(contract_edges, Sequence):
        raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: producer_consumer.contract_edges must be a sequence")
    unresolved_edges: list[str] = []
    for idx, edge in enumerate(contract_edges):
        if not isinstance(edge, Mapping):
            raise ValueError(f"{TASK9_CONTRACT_ERROR_CODE}: producer_consumer.contract_edges[{idx}] must be a mapping")
        edge_id = str(edge.get("edge_id") or f"edge[{idx}]").strip()
        producer_accepts = bool(edge.get("producer_contract_valid"))
        consumer_accepts = bool(edge.get("consumer_contract_valid"))
        integration_validated = bool(edge.get("integration_validated"))
        if not (producer_accepts and consumer_accepts and integration_validated):
            unresolved_edges.append(edge_id)

    _check(
        "9.contract_edges.validated",
        not unresolved_edges,
        f"TASK9_CONTRACT_EDGE_VIOLATION: unresolved producer/consumer contract edges={unresolved_edges}",
    )

    _check(
        "9.rollback.drill_executed",
        bool(rollback.get("drill_executed")),
        "TASK9_ROLLBACK_DRILL_MISSING: rollback drill must be executed before release",
    )
    _check(
        "9.rollback.recovered",
        bool(rollback.get("restore_successful")) and bool(rollback.get("post_rollback_validation_passed")),
        "TASK9_ROLLBACK_RECOVERY_FAILED: rollback must restore stable state and pass post-validation",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK9_RELEASE_HARDENING_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "critical_pass_rate": round(critical_rate, 3),
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        raise ValueError(
            f"{TASK9_GATE_ERROR_CODE}: Task 9 release hardening gate failed checks {failed_checks}"
        )
    return report


def evaluate_task7_v3_final_release_gate(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate final v3 release readiness gates are strictly blocking on critical defects."""

    required_sections = (
        "release_gate",
        "correctness_critical_checks",
        "targeted_pytest_matrix",
        "producer_consumer",
        "contract_drift",
        "startup_gates",
        "rollback",
    )
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    release_gate = evidence["release_gate"]
    critical_checks = evidence["correctness_critical_checks"]
    pytest_matrix = evidence["targeted_pytest_matrix"]
    producer_consumer = evidence["producer_consumer"]
    contract_drift = evidence["contract_drift"]
    startup_gates = evidence["startup_gates"]
    rollback = evidence["rollback"]

    if not isinstance(release_gate, Mapping):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: release_gate must be a mapping")
    if not isinstance(critical_checks, Sequence):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: correctness_critical_checks must be a sequence")
    if not isinstance(pytest_matrix, Sequence):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: targeted_pytest_matrix must be a sequence")
    if not isinstance(producer_consumer, Mapping):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: producer_consumer must be a mapping")
    if not isinstance(contract_drift, Mapping):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: contract_drift must be a mapping")
    if not isinstance(startup_gates, Mapping):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: startup_gates must be a mapping")
    if not isinstance(rollback, Mapping):
        raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: rollback must be a mapping")

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    _check(
        "7.release.block_on_critical_failure",
        bool(release_gate.get("block_on_critical_failure")),
        "TASK7_RELEASE_POLICY_INVALID: block_on_critical_failure must be true",
    )
    _check(
        "7.release.no_warning_only_critical",
        not bool(release_gate.get("allow_warning_only_critical_checks")),
        "TASK7_WARNING_ONLY_CRITICAL_NOT_ALLOWED: critical checks cannot be warning-only",
    )

    critical_rows: dict[str, Mapping[str, Any]] = {}
    for idx, row in enumerate(critical_checks):
        if not isinstance(row, Mapping):
            raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: correctness_critical_checks[{idx}] must be a mapping")
        check_id = str(row.get("check_id") or "").strip()
        if not check_id:
            raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: correctness_critical_checks[{idx}] missing check_id")
        critical_rows[check_id] = row

    missing_critical = sorted(set(_TASK7_REQUIRED_CRITICAL_CHECKS) - set(critical_rows))
    _check(
        "7.critical.required_set",
        not missing_critical,
        f"TASK7_CRITICAL_CHECKSET_INCOMPLETE: missing required checks={missing_critical}",
    )
    for critical_id in sorted(_TASK7_REQUIRED_CRITICAL_CHECKS):
        row = critical_rows.get(critical_id, {})
        status = str(row.get("status") or "").strip().lower()
        is_critical = bool(row.get("critical"))
        warning_only = bool(row.get("warning_only"))
        _check(
            f"7.critical.{critical_id}.critical",
            is_critical,
            f"TASK7_CRITICAL_FLAG_MISSING: {critical_id} must be marked critical",
        )
        _check(
            f"7.critical.{critical_id}.status",
            status == "passed",
            f"TASK7_CRITICAL_CHECK_FAILED: {critical_id} status={status or '<missing>'}",
        )
        _check(
            f"7.critical.{critical_id}.warning",
            not warning_only,
            f"TASK7_WARNING_ONLY_CRITICAL_NOT_ALLOWED: {critical_id} is warning-only",
        )

    matrix_rows: dict[str, Mapping[str, Any]] = {}
    for idx, row in enumerate(pytest_matrix):
        if not isinstance(row, Mapping):
            raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: targeted_pytest_matrix[{idx}] must be a mapping")
        matrix_id = str(row.get("matrix_id") or "").strip()
        if not matrix_id:
            raise ValueError(f"{TASK7_CONTRACT_ERROR_CODE}: targeted_pytest_matrix[{idx}] missing matrix_id")
        matrix_rows[matrix_id] = row

    required_matrix_ids = {
        "monthly",
        "rt",
        "backfill",
        "bootstrap",
        "training",
        "deployment",
    }
    missing_matrix_ids = sorted(required_matrix_ids - set(matrix_rows))
    _check(
        "7.pytest.required_matrix",
        not missing_matrix_ids,
        f"TASK7_TARGETED_MATRIX_INCOMPLETE: missing matrix entries={missing_matrix_ids}",
    )
    for matrix_id in sorted(required_matrix_ids):
        row = matrix_rows.get(matrix_id, {})
        status = str(row.get("status") or "").strip().lower()
        flows = {str(v).strip() for v in (row.get("flows_covered") or []) if str(v).strip()}
        _check(
            f"7.pytest.{matrix_id}.status",
            status == "passed",
            f"TASK7_TARGETED_MATRIX_FAILED: matrix_id={matrix_id} status={status or '<missing>'}",
        )
        _check(
            f"7.pytest.{matrix_id}.flow_coverage",
            flows >= set(_TASK7_REQUIRED_FLOWS),
            f"TASK7_FLOW_COVERAGE_GAP: matrix_id={matrix_id} missing flows={sorted(set(_TASK7_REQUIRED_FLOWS)-flows)}",
        )

    _check(
        "7.producer_consumer.verified",
        bool(producer_consumer.get("interfaces_verified")) and not bool(producer_consumer.get("drift_detected")),
        "TASK7_PRODUCER_CONSUMER_NOT_ALIGNED: producer/consumer interfaces must be verified with no drift",
    )

    _check(
        "7.contract_drift.v3_green",
        bool(contract_drift.get("check_contract_drift_v3_passed")),
        "TASK7_CONTRACT_DRIFT_NOT_BLOCKED: v3 contract drift check must pass",
    )

    startup_statuses = {
        gate_name: str((startup_gates.get(gate_name) or {}).get("status") or "").strip().lower()
        for gate_name in _TASK7_REQUIRED_STARTUP_GATES
    }
    for gate_name, status in startup_statuses.items():
        _check(
            f"7.startup.{gate_name}",
            status == "go",
            f"TASK7_STARTUP_GATE_RED: gate={gate_name} status={status or '<missing>'}",
        )

    _check(
        "7.rollback.drill",
        bool(rollback.get("drill_executed")) and bool(rollback.get("restore_successful")),
        "TASK7_ROLLBACK_NOT_READY: rollback drill and restoration must be successful",
    )
    _check(
        "7.rollback.deterministic",
        bool(rollback.get("deterministic_replay_verified")),
        "TASK7_ROLLBACK_NON_DETERMINISTIC: deterministic replay safety must be validated",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK7_FINAL_RELEASE_GATE_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        raise ValueError(
            f"{TASK7_GATE_ERROR_CODE}: Task 7 final release gate failed checks {failed_checks}"
        )
    return report


def determine_task7_artifact_rollback_action(*, failure_stage: str, reason_code: str) -> dict[str, Any]:
    normalized_stage = (failure_stage or "").strip().lower()
    if normalized_stage not in {"validate", "smoke", "promotion"}:
        raise ValueError(
            f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: failure_stage must be validate|smoke|promotion, "
            f"got {failure_stage!r}"
        )
    normalized_reason = (reason_code or "").strip()
    if not normalized_reason:
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: reason_code must be non-empty")

    stage_to_actions = {
        "validate": ["block_promotion", "keep_previous_contract_pointers", "emit_non_retriable_alert"],
        "smoke": ["rollback_promoted_contract_pointers", "restore_last_known_good_artifacts", "open_incident"],
        "promotion": ["rollback_promoted_contract_pointers", "reconcile_ddb_contract_state", "open_incident"],
    }
    return {
        "status": "ROLLBACK_REQUIRED",
        "error_code": TASK7_ARTIFACT_ROLLBACK_ERROR_CODE,
        "failure_stage": normalized_stage,
        "reason_code": normalized_reason,
        "actions": stage_to_actions[normalized_stage],
    }


def evaluate_task7_code_artifact_lifecycle_gate(*, evidence: Mapping[str, Any]) -> dict[str, Any]:
    required_sections = (
        "release_gate",
        "lifecycle",
        "targeted_pytest_matrix",
        "contract_drift",
        "pipeline_definitions",
        "notebook_checks",
        "producer_consumer",
        "rollback",
    )
    missing_sections = [section for section in required_sections if section not in evidence]
    if missing_sections:
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: missing required sections {missing_sections}")

    release_gate = evidence["release_gate"]
    lifecycle = evidence["lifecycle"]
    matrix = evidence["targeted_pytest_matrix"]
    contract_drift = evidence["contract_drift"]
    pipeline_definitions = evidence["pipeline_definitions"]
    notebook_checks = evidence["notebook_checks"]
    producer_consumer = evidence["producer_consumer"]
    rollback = evidence["rollback"]

    if not isinstance(release_gate, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: release_gate must be a mapping")
    if not isinstance(lifecycle, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: lifecycle must be a mapping")
    if not isinstance(matrix, Sequence):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: targeted_pytest_matrix must be a sequence")
    if not isinstance(contract_drift, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: contract_drift must be a mapping")
    if not isinstance(pipeline_definitions, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: pipeline_definitions must be a mapping")
    if not isinstance(notebook_checks, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: notebook_checks must be a mapping")
    if not isinstance(producer_consumer, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: producer_consumer must be a mapping")
    if not isinstance(rollback, Mapping):
        raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: rollback must be a mapping")

    checks: list[dict[str, Any]] = []

    def _check(check_id: str, passed: bool, detail: str) -> None:
        checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})

    _check(
        "7a.release.block_on_failure",
        bool(release_gate.get("block_on_failure")),
        "TASK7_RELEASE_POLICY_INVALID: block_on_failure must be true",
    )
    _check(
        "7a.lifecycle.build",
        str(lifecycle.get("build_status") or "").upper() == "PASS",
        f"TASK7_LIFECYCLE_BUILD_FAILED: build_status={lifecycle.get('build_status')!r}",
    )
    _check(
        "7a.lifecycle.validate",
        str(lifecycle.get("validate_status") or "").upper() == "PASS",
        f"TASK7_LIFECYCLE_VALIDATE_FAILED: validate_status={lifecycle.get('validate_status')!r}",
    )
    _check(
        "7a.lifecycle.smoke",
        str(lifecycle.get("smoke_status") or "").upper() == "PASS",
        f"TASK7_LIFECYCLE_SMOKE_FAILED: smoke_status={lifecycle.get('smoke_status')!r}",
    )
    _check(
        "7a.lifecycle.promoted_contract_ready",
        bool(lifecycle.get("promoted_contract_ready")),
        "TASK7_PROMOTED_CONTRACT_NOT_READY: promoted contract readiness must be true",
    )

    matrix_rows: dict[str, Mapping[str, Any]] = {}
    for idx, row in enumerate(matrix):
        if not isinstance(row, Mapping):
            raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: targeted_pytest_matrix[{idx}] must be a mapping")
        family = str(row.get("artifact_family") or "").strip()
        if not family:
            raise ValueError(f"{TASK7_ARTIFACT_CONTRACT_ERROR_CODE}: targeted_pytest_matrix[{idx}] missing artifact_family")
        matrix_rows[family] = row

    missing_families = sorted(set(_TASK7_ARTIFACT_REQUIRED_FAMILIES) - set(matrix_rows))
    _check(
        "7a.matrix.family_coverage",
        not missing_families,
        f"TASK7_FAMILY_COVERAGE_GAP: missing families={missing_families}",
    )
    for family in _TASK7_ARTIFACT_REQUIRED_FAMILIES:
        row = matrix_rows.get(family, {})
        status = str(row.get("status") or "").lower()
        _check(
            f"7a.matrix.{family}.status",
            status == "passed",
            f"TASK7_MATRIX_FAMILY_FAILED: family={family} status={status or '<missing>'}",
        )
        _check(
            f"7a.matrix.{family}.consumption",
            bool(row.get("runtime_consumption_verified")),
            f"TASK7_RUNTIME_CONSUMPTION_NOT_VERIFIED: family={family}",
        )
        _check(
            f"7a.matrix.{family}.retry_replay",
            bool(row.get("retry_replay_deterministic")),
            f"TASK7_RETRY_REPLAY_NOT_DETERMINISTIC: family={family}",
        )

    _check(
        "7a.contract_drift.v3",
        bool(contract_drift.get("check_contract_drift_v3_passed")),
        "TASK7_CONTRACT_DRIFT_NOT_BLOCKED: contract drift check must pass",
    )
    _check(
        "7a.pipeline_definitions",
        bool(pipeline_definitions.get("checks_passed")),
        "TASK7_PIPELINE_DEFINITION_CHECK_FAILED: pipeline definition checks must pass",
    )
    _check(
        "7a.notebook.structure",
        bool(notebook_checks.get("structure_passed")),
        "TASK7_NOTEBOOK_STRUCTURE_CHECK_FAILED: notebook structure check must pass",
    )
    _check(
        "7a.notebook.parity",
        bool(notebook_checks.get("parity_passed")),
        "TASK7_NOTEBOOK_PARITY_CHECK_FAILED: notebook parity check must pass",
    )
    _check(
        "7a.producer_consumer.alignment",
        bool(producer_consumer.get("interfaces_verified")) and not bool(producer_consumer.get("drift_detected")),
        "TASK7_PRODUCER_CONSUMER_NOT_ALIGNED: interfaces must be verified with no drift",
    )
    _check(
        "7a.rollback.failed_validation",
        bool(rollback.get("failed_validation_rollback_tested")),
        "TASK7_ROLLBACK_VALIDATION_PATH_NOT_TESTED: failed-validation rollback must be tested",
    )
    _check(
        "7a.rollback.failed_smoke",
        bool(rollback.get("failed_smoke_rollback_tested")),
        "TASK7_ROLLBACK_SMOKE_PATH_NOT_TESTED: failed-smoke rollback must be tested",
    )
    _check(
        "7a.rollback.deterministic_replay",
        bool(rollback.get("deterministic_replay_verified")),
        "TASK7_ROLLBACK_REPLAY_NOT_DETERMINISTIC: rollback replay determinism must be verified",
    )
    _check(
        "7a.rollback.playbook_documented",
        bool(rollback.get("playbook_documented")),
        "TASK7_ROLLBACK_PLAYBOOK_MISSING: rollback playbook evidence is required",
    )

    failed_checks = [check["check_id"] for check in checks if not check["passed"]]
    report = {
        "contract_version": TASK7_ARTIFACT_LIFECYCLE_GATE_VERSION,
        "status": "go" if not failed_checks else "no-go",
        "checks": checks,
        "failed_checks": failed_checks,
    }
    if failed_checks:
        raise ValueError(
            f"{TASK7_ARTIFACT_GATE_ERROR_CODE}: Task 7 artifact lifecycle gate failed checks {failed_checks}"
        )
    return report
