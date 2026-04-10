"""Shared raw input resolution for ingestion and Redshift fallback paths."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ndr.processing.backfill_redshift_fallback import (
    BackfillRange,
    execute_backfill_redshift_fallback,
    load_backfill_fallback_contract,
)


ERROR_CODE_FALLBACK_DISABLED = "RAW_INPUT_FALLBACK_DISABLED"
ERROR_CODE_FALLBACK_QUERY_CONTRACT_MISSING = "RAW_INPUT_FALLBACK_QUERY_CONTRACT_MISSING"
ERROR_CODE_FALLBACK_EMPTY_RESULT = "RAW_INPUT_FALLBACK_EMPTY_RESULT"


@dataclass(frozen=True)
class RawInputResolution:
    """Deterministic raw input source decision consumed by readers/status writers."""

    source_mode: str
    raw_input_s3_prefix: str
    resolution_reason: str
    provenance: dict[str, str]


class RawInputResolver:
    """Resolves ingestion-vs-Redshift source selection from DPP-owned contracts only."""

    def resolve(
        self,
        *,
        ingestion_rows: list[dict[str, Any]],
        allow_redshift_fallback: bool,
        dpp_spec: dict[str, Any],
        artifact_family: str,
        range_start_ts: str,
        range_end_ts: str,
        producer_flow: str,
        request_id: str | None = None,
    ) -> RawInputResolution:
        ingestion_prefix = self._first_ingestion_prefix(ingestion_rows)
        if ingestion_prefix:
            return RawInputResolution(
                source_mode="ingestion",
                raw_input_s3_prefix=ingestion_prefix,
                resolution_reason="ingestion_rows_present",
                provenance=_build_provenance(
                    source_mode="ingestion",
                    producer_flow=producer_flow,
                    request_id=request_id,
                    resolution_reason="ingestion_rows_present",
                ),
            )

        if not allow_redshift_fallback:
            raise RuntimeError(
                f"{ERROR_CODE_FALLBACK_DISABLED}: No ingestion rows resolved and Redshift fallback is disabled"
            )

        try:
            redshift_config, query_spec = load_backfill_fallback_contract(
                dpp_spec=dpp_spec,
                artifact_family=artifact_family,
            )
        except ValueError as exc:
            raise RuntimeError(
                f"{ERROR_CODE_FALLBACK_QUERY_CONTRACT_MISSING}: {exc}"
            ) from exc
        fallback_results = execute_backfill_redshift_fallback(
            config=redshift_config,
            query_spec=query_spec,
            ranges=[BackfillRange(start_ts=range_start_ts, end_ts=range_end_ts)],
        )
        if not fallback_results:
            raise RuntimeError(
                f"{ERROR_CODE_FALLBACK_EMPTY_RESULT}: fallback execution returned no ranges"
            )

        fallback_prefix = fallback_results[0].unload_s3_prefix
        return RawInputResolution(
            source_mode="redshift_unload_fallback",
            raw_input_s3_prefix=fallback_prefix,
            resolution_reason="ingestion_rows_missing",
            provenance=_build_provenance(
                source_mode="redshift_unload_fallback",
                producer_flow=producer_flow,
                request_id=request_id,
                resolution_reason="ingestion_rows_missing",
            ),
        )

    @staticmethod
    def _first_ingestion_prefix(ingestion_rows: list[dict[str, Any]]) -> str:
        for row in ingestion_rows:
            candidate = str(row.get("raw_parsed_logs_s3_prefix", "")).strip()
            if candidate:
                return candidate
        return ""


def _build_provenance(
    *,
    source_mode: str,
    producer_flow: str,
    resolution_reason: str,
    request_id: str | None,
) -> dict[str, str]:
    return {
        "source_mode": source_mode,
        "producer_flow": producer_flow,
        "resolution_reason": resolution_reason,
        "request_id": request_id or "",
        "resolved_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
