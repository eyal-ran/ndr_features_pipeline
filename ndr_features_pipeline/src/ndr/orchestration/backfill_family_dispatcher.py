"""Backfill family dispatch + strict completion verification contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import boto3

from ndr.orchestration.backfill_contracts import ARTIFACT_FAMILY_ORDER

DISPATCHER_ERROR_CODE = "BACKFILL_DISPATCH_CONTRACT_ERROR"
COMPLETION_ERROR_CODE = "BACKFILL_COMPLETION_CONTRACT_ERROR"

_SUCCESS_STATUSES = {"Started", "Succeeded", "DuplicateRequest"}
_RETRYABLE_START_ERRORS = ("ThrottlingException", "ServiceUnavailable", "InternalFailure")


@dataclass(frozen=True)
class MissingEntry:
    family: str
    window_start_ts: str
    window_end_ts: str
    batch_ids: tuple[str, ...]
    required_inputs: tuple[str, ...]
    reason_code: str


class BackfillFamilyDispatcher:
    """Dispatches canonical family handlers with deterministic payloads and idempotent replay support."""

    def __init__(
        self,
        *,
        project_name: str,
        feature_spec_version: str,
        requested_families: tuple[str, ...],
        correlation_id: str,
        retry_attempt: int,
        dpp_spec: dict[str, Any],
        sagemaker_client: Any | None = None,
    ) -> None:
        self.project_name = project_name
        self.feature_spec_version = feature_spec_version
        unknown = sorted(set(requested_families) - set(ARTIFACT_FAMILY_ORDER))
        if unknown:
            raise ValueError(
                f"{DISPATCHER_ERROR_CODE}: unsupported requested_families={unknown}"
            )
        self.requested_families = tuple(f for f in ARTIFACT_FAMILY_ORDER if f in requested_families)
        self.correlation_id = correlation_id
        self.retry_attempt = retry_attempt
        self.dpp_spec = dpp_spec
        self.sagemaker_client = sagemaker_client or boto3.client("sagemaker")

    def dispatch(self, *, missing_entries: list[dict[str, Any]]) -> dict[str, Any]:
        entries = _normalize_missing_entries(missing_entries)
        grouped = _group_entries_by_family(entries=entries)

        family_results: list[dict[str, Any]] = []
        for family in ARTIFACT_FAMILY_ORDER:
            if family not in self.requested_families:
                continue
            payload = _build_family_payload(
                project_name=self.project_name,
                feature_spec_version=self.feature_spec_version,
                family=family,
                entries=grouped.get(family, ()),
                correlation_id=self.correlation_id,
                retry_attempt=self.retry_attempt,
            )
            family_results.append(self._dispatch_family(payload=payload))

        verifier = BackfillCompletionVerifier(
            requested_families=self.requested_families,
            family_results=family_results,
        )
        verdict = verifier.verify()
        return {
            "family_results": family_results,
            "completion": verdict,
        }

    def _dispatch_family(self, *, payload: dict[str, Any]) -> dict[str, Any]:
        family = payload["family"]
        pipeline_name = _resolve_target_pipeline_name(dpp_spec=self.dpp_spec, family=family)
        client_request_token = f"{self.correlation_id}-{family}-r{self.retry_attempt}"[:64]

        parameters = [
            {"Name": "ProjectName", "Value": self.project_name},
            {"Name": "FeatureSpecVersion", "Value": self.feature_spec_version},
            {"Name": "ArtifactFamily", "Value": family},
            {"Name": "RangeStartTsIso", "Value": payload["window_ranges"][0]["start_ts"]},
            {"Name": "RangeEndTsIso", "Value": payload["window_ranges"][-1]["end_ts"]},
            {"Name": "IdempotencyKey", "Value": self.correlation_id},
        ]

        try:
            response = self.sagemaker_client.start_pipeline_execution(
                PipelineName=pipeline_name,
                PipelineExecutionDescription=(
                    f"backfill_dispatch family={family} correlation_id={self.correlation_id} retry={self.retry_attempt}"
                ),
                PipelineParameters=parameters,
                ClientRequestToken=client_request_token,
            )
            return {
                "family": family,
                "status": "Started",
                "execution_arn": response["PipelineExecutionArn"],
                "produced_artifacts": [],
                "failure_reason": "",
                "retryable": False,
                "handler_payload": payload,
            }
        except Exception as exc:  # pragma: no cover - exercised by tests via mocked client exceptions
            text = str(exc)
            if "ConflictException" in text or "already exists" in text.lower():
                return {
                    "family": family,
                    "status": "DuplicateRequest",
                    "execution_arn": "",
                    "produced_artifacts": [],
                    "failure_reason": "EXECUTION_ALREADY_EXISTS",
                    "retryable": False,
                    "handler_payload": payload,
                }
            retryable = any(code in text for code in _RETRYABLE_START_ERRORS)
            return {
                "family": family,
                "status": "Failed",
                "execution_arn": "",
                "produced_artifacts": [],
                "failure_reason": f"DISPATCH_FAILED:{text}",
                "retryable": retryable,
                "handler_payload": payload,
            }


class BackfillCompletionVerifier:
    """Verifies completion contract truthfully before completion publication."""

    def __init__(self, *, requested_families: tuple[str, ...], family_results: list[dict[str, Any]]) -> None:
        unknown = sorted(set(requested_families) - set(ARTIFACT_FAMILY_ORDER))
        if unknown:
            raise ValueError(
                f"{DISPATCHER_ERROR_CODE}: unsupported requested_families={unknown}"
            )
        self.requested_families = tuple(f for f in ARTIFACT_FAMILY_ORDER if f in requested_families)
        self.family_results = family_results

    def verify(self) -> dict[str, Any]:
        status_by_family = {item["family"]: item.get("status", "") for item in self.family_results}
        missing = [family for family in self.requested_families if family not in status_by_family]
        failed = [
            family
            for family in self.requested_families
            if family in status_by_family and status_by_family[family] not in _SUCCESS_STATUSES
        ]
        all_succeeded = not missing and not failed
        executed_families = [item.get("family") for item in self.family_results if item.get("family")]
        return {
            "all_succeeded": all_succeeded,
            "failed_families": failed,
            "unresolved_families": missing,
            "executed_families": executed_families,
            "verifier_code": "BACKFILL_COMPLETION_OK" if all_succeeded else COMPLETION_ERROR_CODE,
            "verified_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }


def _normalize_missing_entries(missing_entries: list[dict[str, Any]]) -> list[MissingEntry]:
    normalized: list[MissingEntry] = []
    for idx, entry in enumerate(missing_entries):
        family = str(entry.get("family") or "").strip()
        if family not in ARTIFACT_FAMILY_ORDER:
            raise ValueError(
                f"{DISPATCHER_ERROR_CODE}: unsupported family in missing_entries[{idx}].family={family!r}"
            )
        start_ts = str(entry.get("window_start_ts") or "").strip()
        end_ts = str(entry.get("window_end_ts") or "").strip()
        if not start_ts or not end_ts:
            raise ValueError(
                f"{DISPATCHER_ERROR_CODE}: missing_entries[{idx}] requires window_start_ts/window_end_ts"
            )
        batch_ids = tuple(sorted({str(item).strip() for item in (entry.get("batch_ids") or []) if str(item).strip()}))
        required_inputs = tuple(
            sorted({str(item).strip() for item in (entry.get("required_inputs") or []) if str(item).strip()})
        )
        reason_code = str(entry.get("reason_code") or "MISSING_ENTRY")
        normalized.append(
            MissingEntry(
                family=family,
                window_start_ts=start_ts,
                window_end_ts=end_ts,
                batch_ids=batch_ids,
                required_inputs=required_inputs,
                reason_code=reason_code,
            )
        )
    normalized.sort(key=lambda item: (ARTIFACT_FAMILY_ORDER.index(item.family), item.window_start_ts, item.window_end_ts))
    return normalized


def _group_entries_by_family(*, entries: list[MissingEntry]) -> dict[str, tuple[MissingEntry, ...]]:
    grouped: dict[str, list[MissingEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.family, []).append(entry)
    return {family: tuple(values) for family, values in grouped.items()}


def _build_family_payload(
    *,
    project_name: str,
    feature_spec_version: str,
    family: str,
    entries: tuple[MissingEntry, ...],
    correlation_id: str,
    retry_attempt: int,
) -> dict[str, Any]:
    if not entries:
        raise ValueError(
            f"{DISPATCHER_ERROR_CODE}: requested family {family} has no normalized missing entries"
        )

    window_ranges = [
        {"start_ts": item.window_start_ts, "end_ts": item.window_end_ts}
        for item in entries
    ]
    batch_ids = sorted({batch_id for item in entries for batch_id in item.batch_ids})
    required_inputs = sorted({value for item in entries for value in item.required_inputs})
    reason_codes = sorted({item.reason_code for item in entries})
    return {
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "family": family,
        "window_ranges": window_ranges,
        "batch_ids": batch_ids,
        "required_inputs": required_inputs,
        "reason_codes": reason_codes,
        "correlation_id": correlation_id,
        "retry_attempt": retry_attempt,
    }


def _resolve_target_pipeline_name(*, dpp_spec: dict[str, Any], family: str) -> str:
    orchestration_targets = dpp_spec.get("orchestration_targets") or {}
    target = str(orchestration_targets.get(family) or "").strip()
    if not target:
        raise ValueError(
            f"{DISPATCHER_ERROR_CODE}: missing orchestration target for family={family} at spec.orchestration_targets.{family}"
        )
    return target
