"""Deterministic readiness checker contracts for monthly and realtime gates."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


class ReadinessContractError(ValueError):
    """Explicit readiness contract violation with stable code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


_MONTHLY_VERSION = "monthly_fg_b_readiness.v2"
_RT_VERSION = "rt_artifact_readiness.v2"


def compute_monthly_fg_b_readiness_v2(
    *,
    required_families: Iterable[str],
    missing_ranges: Iterable[Mapping[str, str]],
    as_of_ts: str | None = None,
) -> dict[str, Any]:
    return _compute_readiness(
        contract_version=_MONTHLY_VERSION,
        required_families=required_families,
        missing_ranges=missing_ranges,
        as_of_ts=as_of_ts,
    )


def compute_rt_artifact_readiness_v2(
    *,
    required_families: Iterable[str],
    missing_ranges: Iterable[Mapping[str, str]],
    idempotency_key: str,
    as_of_ts: str | None = None,
) -> dict[str, Any]:
    if not idempotency_key.strip():
        raise ReadinessContractError("RT_READINESS_MISSING_IDEMPOTENCY_KEY", "idempotency_key is required")
    payload = _compute_readiness(
        contract_version=_RT_VERSION,
        required_families=required_families,
        missing_ranges=missing_ranges,
        as_of_ts=as_of_ts,
    )
    payload["idempotency_key"] = idempotency_key
    return payload


def _compute_readiness(
    *,
    contract_version: str,
    required_families: Iterable[str],
    missing_ranges: Iterable[Mapping[str, str]],
    as_of_ts: str | None,
) -> dict[str, Any]:
    families = sorted({str(f).strip() for f in required_families if str(f).strip()})
    if not families:
        raise ReadinessContractError("READINESS_REQUIRED_FAMILIES_EMPTY", "required_families must not be empty")

    normalized = []
    for idx, item in enumerate(missing_ranges):
        family = str(item.get("family") or "").strip()
        start_ts = str(item.get("start_ts_iso") or "").strip()
        end_ts = str(item.get("end_ts_iso") or "").strip()
        reason_code = str(item.get("reason_code") or "dependency_missing").strip()
        if not family or not start_ts or not end_ts:
            raise ReadinessContractError(
                "READINESS_RANGE_FIELD_MISSING",
                f"missing_ranges[{idx}] requires family/start_ts_iso/end_ts_iso",
            )
        normalized.append(
            {
                "family": family,
                "start_ts_iso": start_ts,
                "end_ts_iso": end_ts,
                "reason_code": reason_code,
            }
        )

    normalized.sort(key=lambda item: (item["family"], item["start_ts_iso"], item["end_ts_iso"], item["reason_code"]))
    ready = len(normalized) == 0
    decision_code = "READY" if ready else "MISSING_DEPENDENCIES"
    payload = {
        "contract_version": contract_version,
        "ready": ready,
        "required_families": families,
        "missing_ranges": normalized,
        "unresolved_count": len(normalized),
        "decision_code": decision_code,
        "as_of_ts": as_of_ts or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return payload
