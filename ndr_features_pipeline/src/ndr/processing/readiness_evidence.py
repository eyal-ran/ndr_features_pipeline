"""Authoritative Batch Index + S3 evidence evaluation for readiness gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable
from urllib.parse import urlparse

import importlib

if TYPE_CHECKING:
    from ndr.config.batch_index_loader import BatchIndexLoader, BatchIndexRecord
from ndr.orchestration.backfill_contracts import FAMILY_DEPENDENCIES

RT_DEPENDENT_FAMILY = "fg_c"
MONTHLY_REQUIRED_FAMILIES: tuple[str, ...] = ("fg_a", "pair_counts")


@dataclass(frozen=True)
class ReadinessEvidence:
    required_families: list[str]
    missing_ranges: list[dict[str, str]]


class S3ArtifactProbe:
    """Small injectable S3 existence adapter used by readiness evaluators."""

    def __init__(self, s3_client: Any | None = None) -> None:
        self._s3 = s3_client or importlib.import_module("boto3").client("s3")

    def exists(self, s3_uri: str) -> bool:
        bucket, key = _split_s3_uri(s3_uri)
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            response = getattr(exc, "response", {})
            status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            code = str(response.get("Error", {}).get("Code", ""))
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True


class RtReadinessEvidenceEvaluator:
    def __init__(
        self,
        *,
        batch_index_loader: BatchIndexLoader | None = None,
        artifact_probe: S3ArtifactProbe | None = None,
    ) -> None:
        self._loader = batch_index_loader or _new_batch_index_loader()
        self._probe = artifact_probe or S3ArtifactProbe()

    def evaluate(
        self,
        *,
        project_name: str,
        ml_project_name: str,
        mini_batch_id: str,
        batch_start_ts_iso: str,
        batch_end_ts_iso: str,
    ) -> ReadinessEvidence:
        required = list(FAMILY_DEPENDENCIES[RT_DEPENDENT_FAMILY])
        record = self._loader.get_batch(
            project_name=project_name, batch_id=mini_batch_id
        )
        if record is None:
            return ReadinessEvidence(
                required,
                _missing_for_all(
                    required,
                    batch_start_ts_iso,
                    batch_end_ts_iso,
                    "batch_index_record_missing",
                ),
            )
        if ml_project_name not in record.s3_prefixes.get("mlp", {}):
            return ReadinessEvidence(
                required,
                _missing_for_all(
                    required,
                    batch_start_ts_iso,
                    batch_end_ts_iso,
                    "ml_project_branch_missing",
                ),
            )
        return ReadinessEvidence(
            required,
            _missing_for_record(
                record, required, batch_start_ts_iso, batch_end_ts_iso, self._probe
            ),
        )


class MonthlyReadinessEvidenceEvaluator:
    def __init__(
        self,
        *,
        batch_index_loader: BatchIndexLoader | None = None,
        artifact_probe: S3ArtifactProbe | None = None,
    ) -> None:
        self._loader = batch_index_loader or _new_batch_index_loader()
        self._probe = artifact_probe or S3ArtifactProbe()

    def evaluate(self, *, project_name: str, reference_month: str) -> ReadinessEvidence:
        required = list(MONTHLY_REQUIRED_FAMILIES)
        start_ts_iso = f"{reference_month.replace('/', '-')}-01T00:00:00Z"
        end_ts_iso = _next_month_iso(reference_month)
        records = self._loader.lookup_forward(
            project_name=project_name,
            data_source_name=project_name,
            version="readiness",
            start_ts_iso=start_ts_iso,
            end_ts_iso=end_ts_iso,
        )
        if not records:
            return ReadinessEvidence(
                required,
                _missing_for_all(
                    required, start_ts_iso, end_ts_iso, "batch_index_records_missing"
                ),
            )
        missing: list[dict[str, str]] = []
        for record in records:
            missing.extend(
                _missing_for_record(
                    record,
                    required,
                    record.etl_ts,
                    _window_end(record.etl_ts),
                    self._probe,
                )
            )
        return ReadinessEvidence(required, _coalesce_ranges(missing))


def _missing_for_record(
    record: BatchIndexRecord,
    families: Iterable[str],
    start_ts: str,
    end_ts: str,
    probe: S3ArtifactProbe,
) -> list[dict[str, str]]:
    dpp = record.s3_prefixes.get("dpp", {})
    missing: list[dict[str, str]] = []
    for family in families:
        uris = _family_uris(dpp, family)
        if not uris:
            missing.append(
                _range(family, start_ts, end_ts, "batch_index_prefix_missing")
            )
        elif not all(probe.exists(uri) for uri in uris):
            missing.append(_range(family, start_ts, end_ts, "artifact_object_missing"))
    return missing


def _family_uris(dpp: dict[str, Any], family: str) -> list[str]:
    value = dpp.get("fg_b" if family == "fg_b_baseline" else family)
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, dict):
        return [
            str(uri) for uri in value.values() if isinstance(uri, str) and uri.strip()
        ]
    return []


def _missing_for_all(
    families: Iterable[str], start_ts: str, end_ts: str, reason: str
) -> list[dict[str, str]]:
    return [_range(family, start_ts, end_ts, reason) for family in families]


def _range(family: str, start_ts: str, end_ts: str, reason: str) -> dict[str, str]:
    return {
        "family": family,
        "start_ts_iso": start_ts,
        "end_ts_iso": end_ts,
        "reason_code": reason,
    }


def _coalesce_ranges(ranges: list[dict[str, str]]) -> list[dict[str, str]]:
    ordered = sorted(
        ranges,
        key=lambda item: (
            item["family"],
            item["reason_code"],
            item["start_ts_iso"],
            item["end_ts_iso"],
        ),
    )
    out: list[dict[str, str]] = []
    for item in ordered:
        if (
            out
            and out[-1]["family"] == item["family"]
            and out[-1]["reason_code"] == item["reason_code"]
            and out[-1]["end_ts_iso"] == item["start_ts_iso"]
        ):
            out[-1]["end_ts_iso"] = item["end_ts_iso"]
        else:
            out.append(dict(item))
    return out


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Expected concrete s3:// URI, got: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _next_month_iso(reference_month: str) -> str:
    year, month = (int(part) for part in reference_month.split("/"))
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    return f"{year:04d}-{month:02d}-01T00:00:00Z"


def _window_end(start_ts_iso: str) -> str:
    from datetime import datetime, timedelta, timezone

    parsed = datetime.fromisoformat(start_ts_iso.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    return (
        (parsed + timedelta(minutes=15))
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _new_batch_index_loader():
    from ndr.config.batch_index_loader import BatchIndexLoader

    return BatchIndexLoader()
