"""Backfill Batch Index dual-item writes with deterministic status and reconciliation."""

from __future__ import annotations

from dataclasses import dataclass

from ndr.config.batch_index_writer import BatchIndexWriteRequest, BatchIndexWriter
from ndr.contracts import date_lookup_sk

BACKFILL_STATUS_SEQUENCE: tuple[str, ...] = (
    "planned",
    "materialized",
    "validated",
    "published",
)


@dataclass(frozen=True)
class BackfillWriteResult:
    project_name: str
    batch_id: str
    idempotency_token: str
    final_status: str
    reconciled: bool


class BackfillBatchIndexWritePath:
    """Contract-safe write path for reconstructed backfill batches (Task 7.4)."""

    def __init__(self, writer: BatchIndexWriter | None = None) -> None:
        self._writer = writer or BatchIndexWriter()

    def materialize_reconstructed_batch(
        self,
        *,
        write_request: BatchIndexWriteRequest,
        idempotency_token: str,
    ) -> BackfillWriteResult:
        if not idempotency_token.strip():
            raise ValueError("idempotency_token is required")
        _validate_branch_prefix_coverage(write_request)

        reconciled = False
        try:
            self._writer.upsert_dual_items(write_request)
            self._reconcile_partial_writes(write_request)
            self._advance_backfill_status(write_request=write_request, target_status="published")
            return BackfillWriteResult(
                project_name=write_request.project_name,
                batch_id=write_request.batch_id,
                idempotency_token=idempotency_token,
                final_status="published",
                reconciled=reconciled,
            )
        except Exception:
            reconciled = self._reconcile_partial_writes(write_request)
            try:
                self._writer.update_status(
                    project_name=write_request.project_name,
                    batch_id=write_request.batch_id,
                    backfill_status="failed",
                )
            except Exception:
                pass
            return BackfillWriteResult(
                project_name=write_request.project_name,
                batch_id=write_request.batch_id,
                idempotency_token=idempotency_token,
                final_status="failed",
                reconciled=reconciled,
            )

    def _reconcile_partial_writes(self, write_request: BatchIndexWriteRequest) -> bool:
        batch_sk = write_request.batch_id
        date_sk = date_lookup_sk(
            date_partition=write_request.date_partition,
            hour=write_request.hour,
            within_hour_run_number=write_request.within_hour_run_number,
        )

        batch_item = self._writer.get_item(project_name=write_request.project_name, sort_key=batch_sk)
        date_item = self._writer.get_item(project_name=write_request.project_name, sort_key=date_sk)
        repaired = False
        if batch_item is None:
            self._writer.upsert_dual_items(write_request)
            repaired = True
        elif date_item is None:
            self._writer.upsert_dual_items(write_request)
            repaired = True
        return repaired

    def _advance_backfill_status(self, *, write_request: BatchIndexWriteRequest, target_status: str) -> None:
        if target_status not in BACKFILL_STATUS_SEQUENCE:
            raise ValueError(f"Unknown backfill target status: {target_status}")
        for status in BACKFILL_STATUS_SEQUENCE:
            self._writer.update_status(
                project_name=write_request.project_name,
                batch_id=write_request.batch_id,
                backfill_status=status,
            )
            if status == target_status:
                break


def _validate_branch_prefix_coverage(write_request: BatchIndexWriteRequest) -> None:
    s3_prefixes = write_request.s3_prefixes or {}
    dpp = s3_prefixes.get("dpp")
    mlp = s3_prefixes.get("mlp")
    if not isinstance(dpp, dict):
        raise ValueError("s3_prefixes.dpp must be present")
    if not isinstance(mlp, dict):
        raise ValueError("s3_prefixes.mlp must be present")

    missing = [ml_project_name for ml_project_name in write_request.ml_project_names if ml_project_name not in mlp]
    if missing:
        raise ValueError(f"s3_prefixes.mlp is missing branch entries for: {sorted(missing)}")
