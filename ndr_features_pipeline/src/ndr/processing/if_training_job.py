"""NDR if training job module."""

from __future__ import annotations


import hashlib
import io
import json
import logging
import tarfile
import time
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from ndr.catalog.feature_catalog import build_fg_c_metric_names
from ndr.catalog.schema_manifest import build_fg_a_manifest, build_fg_c_manifest
from ndr.orchestration.backfill_contracts import (
    build_execution_manifest,
    build_family_range_plan,
)
from ndr.orchestration.training_missing_manifest import ensure_manifest, from_missing_sources
from ndr.processing.base_runner import BaseProcessingJobRunner
from ndr.processing.if_training_preprocessing import split_metadata_and_feature_columns
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig, IFTrainingSpec, parse_if_training_spec
from ndr.processing.schema_enforcement import enforce_schema

logger = logging.getLogger(__name__)


DEFAULT_REMEDIATION_CHUNK_SIZE = 25
TARGET_CONTRACT_ERROR_CODE = "IFTrainingOrchestrationTargetContractError"


class IFTrainingJob(BaseProcessingJobRunner):
    """Data container for IFTrainingJob."""
    def __init__(
        self,
        spark,
        runtime_config: IFTrainingRuntimeConfig,
        training_spec: IFTrainingSpec,
        resolved_spec_payload: Dict[str, Any] | None = None,
    ) -> str:
        """Initialize the instance with required clients and runtime configuration."""
        super().__init__(spark)
        self.runtime_config = runtime_config
        self.training_spec = training_spec
        self.resolved_spec_payload = resolved_spec_payload or {}


    def _validate_runtime_contract(self) -> None:
        """Validate runtime timestamps passed by orchestration before heavy compute starts."""
        required = {
            "training_start_ts": self.runtime_config.training_start_ts,
            "training_end_ts": self.runtime_config.training_end_ts,
            "ml_project_name": self.runtime_config.ml_project_name,
            "dpp_config_table_name": self.runtime_config.dpp_config_table_name,
            "mlp_config_table_name": self.runtime_config.mlp_config_table_name,
            "batch_index_table_name": self.runtime_config.batch_index_table_name,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required unified training runtime parameters: {', '.join(missing)}")

    @staticmethod
    def _parse_iso_ts(value: str) -> datetime:
        """Parse timestamp string and normalize to UTC."""
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    def _resolve_evaluation_windows(self) -> List[Dict[str, Any]]:
        """Resolve evaluation windows from DDB spec, or legacy runtime start/end bounds."""
        windows: List[Dict[str, Any]] = []
        if self.training_spec.evaluation_windows:
            windows = [
                {"window_id": w.window_id, "start_ts": w.start_ts, "end_ts": w.end_ts}
                for w in self.training_spec.evaluation_windows
            ]
        elif self.runtime_config.eval_start_ts and self.runtime_config.eval_end_ts:
            windows = [
                {
                    "window_id": "legacy_window_1",
                    "start_ts": self.runtime_config.eval_start_ts,
                    "end_ts": self.runtime_config.eval_end_ts,
                }
            ]

        normalized: List[Dict[str, Any]] = []
        for idx, window in enumerate(windows, start=1):
            start_ts = window.get("start_ts") or window.get("eval_start_ts")
            end_ts = window.get("end_ts") or window.get("eval_end_ts")
            if not start_ts or not end_ts:
                raise ValueError("Each evaluation window must include start_ts and end_ts")
            start_dt = self._parse_iso_ts(start_ts)
            end_dt = self._parse_iso_ts(end_ts)
            if start_dt >= end_dt:
                raise ValueError("Evaluation window start_ts must be earlier than end_ts")
            normalized.append(
                {
                    "window_id": str(window.get("window_id") or f"window_{idx}"),
                    "start_ts": start_dt.isoformat().replace("+00:00", "Z"),
                    "end_ts": end_dt.isoformat().replace("+00:00", "Z"),
                }
            )
        for idx in range(1, len(normalized)):
            prior_end = self._parse_iso_ts(normalized[idx - 1]["end_ts"])
            current_start = self._parse_iso_ts(normalized[idx]["start_ts"])
            if current_start < prior_end:
                raise ValueError("Evaluation windows must be non-overlapping and sorted by time")
        return normalized

    def _get_orchestration_target_overrides(self) -> Dict[str, str]:
        """Read orchestration target overrides from resolved IF-training JobSpec payload."""
        payload_candidates = [
            self.resolved_spec_payload.get("orchestration_targets"),
            (self.resolved_spec_payload.get("runtime") or {}).get("orchestration_targets"),
            (self.resolved_spec_payload.get("pipeline_defaults") or {}).get("orchestration_targets"),
            (self.resolved_spec_payload.get("orchestrators") or {}),
        ]
        for candidate in payload_candidates:
            if isinstance(candidate, dict):
                return {str(k): str(v) for k, v in candidate.items() if v}
        return {}

    def _resolve_state_machine_arn_from_name(self, sfn_client, state_machine_name: str) -> str | None:
        """Resolve a Step Functions state machine ARN from a state machine name."""
        next_token = None
        while True:
            kwargs = {"maxResults": 100}
            if next_token:
                kwargs["nextToken"] = next_token
            response = sfn_client.list_state_machines(**kwargs)
            for item in response.get("stateMachines", []):
                if item.get("name") == state_machine_name:
                    return item.get("stateMachineArn")
            next_token = response.get("nextToken")
            if not next_token:
                break
        return None

    def _resolve_orchestration_target(
        self,
        *,
        family: str,
        sfn_client,
        sagemaker_client,
        required: bool,
    ) -> Dict[str, Any]:
        """Resolve orchestrator target strictly from DDB contract with provenance."""
        family_defaults = {
            "backfill_15m": {
                "type": "stepfunctions",
            },
            "fg_b_baseline": {
                "type": "sagemaker_pipeline",
            },
            "inference": {
                "type": "sagemaker_pipeline",
            },
            "prediction_feature_join": {
                "type": "sagemaker_pipeline",
            },
        }
        if family not in family_defaults:
            raise ValueError(f"Unknown orchestration target family: {family}")

        defaults = family_defaults[family]
        overrides = self._get_orchestration_target_overrides()
        ddb_value = overrides.get(family)
        if required and not ddb_value:
            raise ValueError(
                f"{TARGET_CONTRACT_ERROR_CODE}: missing required DDB orchestration target for family '{family}' "
                f"at if_training.spec.orchestration_targets.{family}"
            )

        selected = ddb_value
        source = "ddb_contract"

        resolved_value = selected
        resolved_kind = "name" if selected else "unresolved"
        if defaults["type"] == "stepfunctions" and selected and not selected.startswith("arn:"):
            resolved_arn = self._resolve_state_machine_arn_from_name(sfn_client, selected)
            if resolved_arn:
                resolved_value = resolved_arn
                resolved_kind = "arn"
            elif required:
                raise ValueError(
                    f"{TARGET_CONTRACT_ERROR_CODE}: unable to resolve Step Functions state machine name "
                    f"'{selected}' for family '{family}'"
                )
        elif selected and selected.startswith("arn:"):
            resolved_kind = "arn"

        result = {
            "family": family,
            "target_type": defaults["type"],
            "requested_target": selected,
            "resolved_target": resolved_value,
            "resolved_target_kind": resolved_kind,
            "resolution_source": source,
            "required": required,
        }
        if required and not resolved_value:
            raise ValueError(
                f"{TARGET_CONTRACT_ERROR_CODE}: required orchestration target resolved empty value for family '{family}'"
            )
        return result

    def _run_dependency_readiness_gate(
        self,
        *,
        stage: str,
        missing_15m_manifest: List[Dict[str, Any]] | None = None,
        missing_fgb_manifest: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        """Validate external orchestration dependencies before expensive execution."""
        import boto3

        missing_15m_manifest = missing_15m_manifest or []
        missing_fgb_manifest = missing_fgb_manifest or []
        sfn = boto3.client("stepfunctions")
        sagemaker = boto3.client("sagemaker")

        checks: List[Dict[str, Any]] = []

        backfill_required = bool(missing_15m_manifest) and self._resolve_toggle(
            self.training_spec.toggles.enable_auto_remediate_15m and self.training_spec.remediation.enable_backfill_15m,
        )
        fgb_required = bool(missing_fgb_manifest) and self._resolve_toggle(
            self.training_spec.toggles.enable_auto_remediate_fgb and self.training_spec.remediation.enable_fgb_rebuild,
        )
        evaluation_enabled = self._resolve_toggle(
            self.training_spec.toggles.enable_post_training_evaluation,
        )
        join_required = evaluation_enabled and self._resolve_toggle(
            self.training_spec.toggles.enable_eval_join_publication,
        )

        families = [
            ("backfill_15m", backfill_required),
            ("fg_b_baseline", fgb_required),
            ("inference", evaluation_enabled),
            ("prediction_feature_join", join_required),
        ]

        for family, required in families:
            target = self._resolve_orchestration_target(
                family=family,
                sfn_client=sfn,
                sagemaker_client=sagemaker,
                required=required,
            )
            check = dict(target)
            if not required:
                check.update({"status": "skipped", "reason": "branch_disabled_or_not_required"})
                checks.append(check)
                continue
            try:
                if target["target_type"] == "stepfunctions":
                    sfn.describe_state_machine(stateMachineArn=target["resolved_target"])
                else:
                    sagemaker.describe_pipeline(PipelineName=target["resolved_target"])
                check["status"] = "passed"
            except Exception as exc:  # pylint: disable=broad-except
                check.update(
                    {
                        "status": "failed",
                        "reason": (
                            f"{TARGET_CONTRACT_ERROR_CODE}: readiness validation failed for family "
                            f"'{family}': {exc}"
                        ),
                    }
                )
            checks.append(check)

        readiness = {
            "stage": stage,
            "run_id": self.runtime_config.run_id,
            "project_name": self.runtime_config.project_name,
            "ml_project_name": self.runtime_config.ml_project_name,
            "feature_spec_version": self.runtime_config.feature_spec_version,
            "checks": checks,
            "status": "passed" if all(c["status"] in {"passed", "skipped"} for c in checks) else "failed",
        }
        self._write_stage_status("dependency_readiness", readiness)
        failures = [c for c in checks if c["status"] == "failed"]
        if failures:
            raise ValueError(
                f"{TARGET_CONTRACT_ERROR_CODE}: dependency readiness failed for required dependencies "
                f"{[f['family'] for f in failures]}"
            )
        return readiness

    def _run_verification_stage(self, stage_name: str) -> None:
        """Run deterministic data verification for verify/reverify stages."""
        import boto3

        train_start, train_end = self._resolve_training_window()
        fg_a_df = enforce_schema(self._read_windowed_input("fg_a", train_start, train_end), build_fg_a_manifest(), "fg_a", logger)
        fg_c_df = enforce_schema(
            self._read_windowed_input("fg_c", train_start, train_end),
            build_fg_c_manifest(metrics=build_fg_c_metric_names()),
            "fg_c",
            logger,
        )
        preflight = self._preflight_validation(fg_a_df, fg_c_df, train_start, train_end)
        missing_windows_manifest = self._extract_missing_windows_manifest(
            preflight=preflight,
            train_start=train_start,
            train_end=train_end,
            evaluation_windows=[],
        )
        verification_summary = {
            "stage": stage_name,
            "run_id": self.runtime_config.run_id,
            "needs_remediation": bool(missing_windows_manifest["entries"]),
            "missing_windows_manifest": missing_windows_manifest,
            "preflight": preflight,
        }
        bucket, key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        client = boto3.client("s3")
        base = f"{self._report_run_prefix(key_prefix)}/verification"
        _put_json(client, bucket, f"{base}/{stage_name}.json", verification_summary)
        _put_json(client, bucket, f"{base}/latest_status.json", verification_summary)
        logger.info("Unified training %s stage succeeded", stage_name, extra={"preflight": preflight})

    @staticmethod
    def _resolve_toggle(spec_value: bool, default: bool = True) -> bool:
        """Resolve a DDB-owned toggle with fallback default."""
        if spec_value is None:
            return default
        return bool(spec_value)

    def _compute_history_plan(self, train_start: datetime, train_end: datetime, evaluation_windows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute required historical envelopes and concrete resolved policy values."""
        all_starts = [train_start]
        all_ends = [train_end]
        for window in evaluation_windows:
            all_starts.append(self._parse_iso_ts(window["start_ts"]))
            all_ends.append(self._parse_iso_ts(window["end_ts"]))
        u_start = min(all_starts)
        u_end = max(all_ends)

        fg_a_minutes = int(self.training_spec.history_planner.fg_a_max_lookback_minutes or 24 * 60)
        resolved_horizons = [
            {"horizon_days": 7, "tail_days": 2, "head_days": 2, "provenance": "from_default_constants"},
            {"horizon_days": 30, "tail_days": 7, "head_days": 7, "provenance": "from_default_constants"},
        ]
        max_back = max(h["horizon_days"] + h["tail_days"] + h["head_days"] for h in resolved_horizons)
        min_head = min(h["head_days"] for h in resolved_horizons)

        w_15m_start = u_start - timedelta(minutes=fg_a_minutes)
        b_start = u_start - timedelta(days=max_back)
        b_end = u_end - timedelta(days=min_head)
        w_required_start = min(w_15m_start, b_start)

        readiness = self._derive_batch_index_readiness(
            required_start=w_required_start,
            required_end=u_end,
            baseline_start=u_start,
            baseline_end=u_end,
            preflight=None,
        )
        missing_windows_manifest = readiness["missing_windows_manifest"]

        return {
            "inputs": {
                "training": {"start": train_start.isoformat().replace("+00:00", "Z"), "end": train_end.isoformat().replace("+00:00", "Z")},
                "evaluation_windows": evaluation_windows,
            },
            "resolved_constants": {
                "fg_a_max_lookback_minutes": {"value": fg_a_minutes, "provenance": "from_job_spec" if self.training_spec.history_planner.fg_a_max_lookback_minutes != 24 * 60 else "from_default_constants"},
                "fg_b_horizons": resolved_horizons,
            },
            "computed": {
                "u_start": u_start.isoformat().replace("+00:00", "Z"),
                "u_end": u_end.isoformat().replace("+00:00", "Z"),
                "w_15m": {"start": w_15m_start.isoformat().replace("+00:00", "Z"), "end": u_end.isoformat().replace("+00:00", "Z")},
                "b_start": b_start.isoformat().replace("+00:00", "Z"),
                "b_end": b_end.isoformat().replace("+00:00", "Z"),
                "w_required": {"start": w_required_start.isoformat().replace("+00:00", "Z"), "end": u_end.isoformat().replace("+00:00", "Z")},
            },
            "batch_index_readiness": readiness["training_readiness_manifest"],
            "missing_windows_manifest": missing_windows_manifest,
        }

    def _derive_missing_15m_windows(self, start: datetime, end: datetime) -> List[Dict[str, str]]:
        """Derive missing 15m windows using FG-A partition availability heuristics."""
        from pyspark.sql import functions as F

        fg_a_prefix = self.training_spec.feature_inputs["fg_a"].s3_prefix
        try:
            df = self.spark.read.option("mergeSchema", "true").parquet(fg_a_prefix)
            if "window_end_ts" not in df.columns:
                return []
            available = {
                row["window_end_ts"].astimezone(timezone.utc)
                for row in df.select("window_end_ts")
                .where((F.col("window_end_ts") >= F.lit(start)) & (F.col("window_end_ts") < F.lit(end)))
                .distinct()
                .collect()
            }
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Unable to derive missing 15m windows from FG-A input: %s", exc)
            return []

        missing: List[Dict[str, str]] = []
        cursor = start
        while cursor < end:
            if cursor not in available:
                missing.append({"window_start_ts": cursor.isoformat().replace("+00:00", "Z"), "window_end_ts": (cursor + timedelta(minutes=15)).isoformat().replace("+00:00", "Z")})
            cursor += timedelta(minutes=15)
        return missing

    def _derive_missing_fgb_windows(self, u_start: datetime, u_end: datetime, horizons: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Derive missing FG-B baseline references by checking daily reference rows."""
        from pyspark.sql import functions as F

        fgb_input = self.resolved_spec_payload.get("fg_b_input") or {}
        fg_b_prefix = fgb_input.get("s3_prefix")
        if not fg_b_prefix:
            return []

        earliest_ref = u_start
        latest_ref = u_end
        try:
            df = self.spark.read.option("mergeSchema", "true").parquet(fg_b_prefix)
            if "reference_time" not in df.columns:
                return []
            available = {
                row["reference_time"].date().isoformat()
                for row in df.select("reference_time")
                .where((F.col("reference_time") >= F.lit(earliest_ref)) & (F.col("reference_time") < F.lit(latest_ref)))
                .distinct()
                .collect()
            }
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Unable to derive missing FG-B windows: %s", exc)
            return []

        missing: List[Dict[str, Any]] = []
        cursor = earliest_ref.date()
        while cursor < latest_ref.date():
            key = cursor.isoformat()
            if key not in available:
                missing.append(
                    {
                        "reference_time_iso": f"{key}T00:00:00Z",
                        "horizons": [f"{h['horizon_days']}d" for h in horizons],
                    }
                )
            cursor = cursor + timedelta(days=1)
        return missing

    def _write_history_plan(self, history_plan: Dict[str, Any]) -> None:
        """Persist planner artifact in report storage."""
        import boto3

        bucket, key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        key = f"{self._report_run_prefix(key_prefix)}/history_planner.json"
        _put_json(boto3.client("s3"), bucket, key, history_plan)

    def _report_run_prefix(self, key_prefix: str) -> str:
        """Build report key prefix isolated by ml-project branch and run id."""
        return (
            f"{key_prefix.rstrip('/')}/ml_project_name={self.runtime_config.ml_project_name}/"
            f"run_id={self.runtime_config.run_id}"
        )

    def _artifact_run_prefix(self, key_prefix: str, train_start: datetime, train_end: datetime) -> str:
        """Build artifact key prefix isolated by ml-project branch and run id."""
        return (
            f"{key_prefix.rstrip('/')}/ml_project_name={self.runtime_config.ml_project_name}/"
            f"model_version={self.training_spec.model_version}/"
            f"training_start={train_start.strftime('%Y-%m-%d')}/"
            f"training_end={train_end.strftime('%Y-%m-%d')}/run_id={self.runtime_config.run_id}"
        )

    def _run_remediation_stage(self) -> None:
        """Run selective remediation stage for missing windows."""
        import boto3

        verification = self._load_required_latest_verification_status()
        if not verification.get("needs_remediation", False):
            self._write_stage_status(
                stage="remediation",
                payload={
                    "status": "skipped",
                    "reason": "verification reported no missing windows",
                },
            )
            return
        history_plan = self._load_required_history_plan()
        missing_windows_manifest = ensure_manifest(history_plan.get("missing_windows_manifest"))
        missing_windows = missing_windows_manifest["entries"]
        missing_15m_manifest_all = next((entry["ranges"] for entry in missing_windows if entry["artifact_family"] == "fg_a_15m"), [])
        missing_fgb_manifest_all = next((entry["ranges"] for entry in missing_windows if entry["artifact_family"] == "fg_b_daily"), [])
        max_attempts = max(1, int(self.training_spec.remediation.max_retries))
        chunks = self._build_remediation_chunks(missing_windows)

        dependency_readiness = self._run_dependency_readiness_gate(
            stage="remediate",
            missing_15m_manifest=missing_15m_manifest_all,
            missing_fgb_manifest=missing_fgb_manifest_all,
        )

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        client = boto3.client("s3")
        remediation_records: List[Dict[str, Any]] = []
        sfn = boto3.client("stepfunctions")
        sagemaker = boto3.client("sagemaker")
        for chunk in chunks:
            index = chunk["chunk_index"]
            missing_15m_manifest = next(
                (entry["ranges"] for entry in chunk["entries"] if entry["artifact_family"] == "fg_a_15m"),
                [],
            )
            missing_fgb_manifest = next(
                (entry["ranges"] for entry in chunk["entries"] if entry["artifact_family"] == "fg_b_daily"),
                [],
            )
            key = f"{self._report_run_prefix(report_key_prefix)}/remediation/chunk_{index:04d}.json"
            backfill_enabled = bool(missing_15m_manifest) and self._resolve_toggle(
                self.training_spec.toggles.enable_auto_remediate_15m and self.training_spec.remediation.enable_backfill_15m,
            )
            fgb_enabled = bool(missing_fgb_manifest) and self._resolve_toggle(
                self.training_spec.toggles.enable_auto_remediate_fgb and self.training_spec.remediation.enable_fgb_rebuild,
            )

            payload = {
                "chunk": chunk,
                "mode": "orchestrated-remediation",
                "execution_ts": self.runtime_config.execution_ts_iso,
                "actions": {
                    "backfill_15m_invoked": backfill_enabled,
                    "fgb_rebuild_invoked": fgb_enabled,
                },
                "orchestrators": {
                    "backfill_15m": next((c for c in dependency_readiness["checks"] if c["family"] == "backfill_15m"), {}),
                    "fg_b_baseline": next((c for c in dependency_readiness["checks"] if c["family"] == "fg_b_baseline"), {}),
                },
            }
            if backfill_enabled:
                payload["backfill_execution"] = self._invoke_with_retries(
                    max_attempts=max_attempts,
                    invoke_fn=lambda: self._invoke_backfill_reprocessing(
                        sfn_client=sfn,
                        missing_15m=missing_15m_manifest,
                        chunk_index=index,
                        chunk_hash=chunk["chunk_hash"],
                        target=next((c for c in dependency_readiness["checks"] if c["family"] == "backfill_15m"), {}),
                    ),
                )
            if fgb_enabled:
                payload["fgb_execution"] = self._invoke_with_retries(
                    max_attempts=max_attempts,
                    invoke_fn=lambda: self._invoke_fgb_baseline_rebuild(
                        sagemaker_client=sagemaker,
                        missing_fgb=missing_fgb_manifest,
                        chunk_index=index,
                        chunk_hash=chunk["chunk_hash"],
                        target=next((c for c in dependency_readiness["checks"] if c["family"] == "fg_b_baseline"), {}),
                    ),
                )
            _put_json(client, report_bucket, key, payload)
            remediation_records.append(payload)

        self._write_stage_status(
            stage="remediation",
            payload={
                "status": "completed",
                "chunk_count": len(chunks),
                "max_attempts_per_chunk": max_attempts,
                "missing_windows_manifest": missing_windows_manifest,
                "missing_15m_manifest_count": len(missing_15m_manifest_all),
                "missing_fgb_manifest_count": len(missing_fgb_manifest_all),
                "processed_chunks": chunks,
                "records": remediation_records,
            },
        )
        logger.info(
            "Executing selective remediation stage",
            extra={"chunk_count": len(chunks), "max_attempts_per_chunk": max_attempts, "missing_windows": missing_windows},
        )

    def _resolve_remediation_chunk_size(self) -> int:
        """Resolve deterministic remediation chunk size from resolved spec or fallback default."""
        payload = self.resolved_spec_payload if isinstance(self.resolved_spec_payload, dict) else {}
        remediation_payload = payload.get("remediation")
        if not isinstance(remediation_payload, dict):
            return DEFAULT_REMEDIATION_CHUNK_SIZE
        configured = remediation_payload.get("chunk_size")
        if configured is None:
            return DEFAULT_REMEDIATION_CHUNK_SIZE
        return max(1, int(configured))

    def _build_remediation_chunks(self, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Chunk remediation entries deterministically with stable chunk hash metadata."""
        materialized_ranges: List[Dict[str, Any]] = []
        for entry in entries:
            for range_item in entry.get("ranges", []):
                materialized_ranges.append(
                    {
                        "artifact_family": entry["artifact_family"],
                        "source": entry["source"],
                        "project_name": entry["project_name"],
                        "feature_spec_version": entry["feature_spec_version"],
                        "ml_project_name": entry["ml_project_name"],
                        "run_id": entry["run_id"],
                        "range": {
                            "start_ts_iso": range_item["start_ts_iso"],
                            "end_ts_iso": range_item["end_ts_iso"],
                        },
                    }
                )
        materialized_ranges.sort(
            key=lambda item: (
                item["artifact_family"],
                item["range"]["start_ts_iso"],
                item["range"]["end_ts_iso"],
            )
        )
        chunk_size = self._resolve_remediation_chunk_size()
        chunks: List[Dict[str, Any]] = []
        for start_index in range(0, len(materialized_ranges), chunk_size):
            current_slice = materialized_ranges[start_index:start_index + chunk_size]
            grouped_entries: Dict[str, Dict[str, Any]] = {}
            for item in current_slice:
                family = item["artifact_family"]
                if family not in grouped_entries:
                    grouped_entries[family] = {
                        "artifact_family": family,
                        "ranges": [],
                        "source": item["source"],
                        "ml_project_name": item["ml_project_name"],
                        "project_name": item["project_name"],
                        "feature_spec_version": item["feature_spec_version"],
                        "run_id": item["run_id"],
                    }
                grouped_entries[family]["ranges"].append(dict(item["range"]))
            chunk_entries = [grouped_entries[family] for family in sorted(grouped_entries)]
            chunk_hash = hashlib.sha1(
                json.dumps(chunk_entries, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:16]
            chunks.append(
                {
                    "chunk_index": len(chunks) + 1,
                    "chunk_size": chunk_size,
                    "chunk_hash": chunk_hash,
                    "entry_count": len(chunk_entries),
                    "range_count": len(current_slice),
                    "entries": chunk_entries,
                }
            )
        return chunks

    @staticmethod
    def _invoke_with_retries(*, max_attempts: int, invoke_fn):
        """Retry orchestration calls without changing deterministic invocation identity."""
        last_error: Exception | None = None
        for _ in range(max_attempts):
            try:
                return invoke_fn()
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
        assert last_error is not None
        raise last_error

    def _run_planning_stage(self) -> None:
        """Compute and persist remediation planning artifacts before remediation/train."""
        verification = self._load_required_latest_verification_status()
        train_start, train_end = self._resolve_training_window()
        evaluation_windows = self._resolve_evaluation_windows()
        history_plan = self._compute_history_plan(train_start, train_end, evaluation_windows)
        self._write_history_plan(history_plan)
        planned_manifest = ensure_manifest(history_plan.get("missing_windows_manifest"))
        remediation_plan = {
            "contract_version": "if_training_remediation_plan.v1",
            "run_id": self.runtime_config.run_id,
            "project_name": self.runtime_config.project_name,
            "ml_project_name": self.runtime_config.ml_project_name,
            "chunks": self._build_remediation_chunks(planned_manifest["entries"]),
        }
        self._write_stage_status("training_readiness_manifest", history_plan.get("batch_index_readiness", {}))
        self._write_stage_status("missing_windows_manifest", planned_manifest)
        self._write_stage_status("remediation_plan", remediation_plan)
        self._write_stage_status(
            "planning",
            {
                "status": "completed",
                "run_id": self.runtime_config.run_id,
                "verification_stage": verification.get("stage"),
                "verification_needs_remediation": bool(verification.get("needs_remediation")),
                "missing_windows_count": len(ensure_manifest(verification.get("missing_windows_manifest"))["entries"]),
                "planned_missing_windows_count": len(planned_manifest["entries"]),
            },
        )

    def _load_required_latest_verification_status(self) -> Dict[str, Any]:
        """Load verification status and fail fast if verify/reverify has not run."""
        verification = self._load_stage_status("verification/latest_status")
        if not isinstance(verification, dict):
            raise ValueError("verification/latest_status artifact is malformed")
        if not verification.get("stage"):
            raise ValueError("verification/latest_status must include stage metadata")
        ensure_manifest(verification.get("missing_windows_manifest"))
        return verification

    def _load_required_history_plan(self) -> Dict[str, Any]:
        """Load planning artifact and fail fast when absent."""
        history_plan = self._load_stage_status("history_planner")
        if not isinstance(history_plan, dict):
            raise ValueError("history_planner artifact is malformed")
        ensure_manifest(history_plan.get("missing_windows_manifest"))
        return history_plan

    def _enforce_pre_train_reverify_gate(self) -> Dict[str, Any]:
        """Hard gate training on successful reverify with no unresolved windows."""
        verification = self._load_required_latest_verification_status()
        stage_name = str(verification.get("stage") or "")
        unresolved = ensure_manifest(verification.get("missing_windows_manifest"))["entries"]
        needs_remediation = bool(verification.get("needs_remediation"))
        gate_payload = {
            "status": "passed",
            "source_stage": stage_name,
            "needs_remediation": needs_remediation,
            "unresolved_windows": unresolved,
        }
        if stage_name != "reverify":
            gate_payload["status"] = "failed"
            gate_payload["reason"] = "reverify stage artifact is required before train"
            self._write_stage_status("train_gate", gate_payload)
            raise ValueError("Train stage requires completed reverify artifact before execution")
        if needs_remediation or unresolved:
            gate_payload["status"] = "failed"
            gate_payload["reason"] = "unresolved required windows remain after reverify"
            self._write_stage_status("train_gate", gate_payload)
            raise ValueError("Train stage blocked: unresolved required windows after reverify")
        self._write_stage_status("train_gate", gate_payload)
        return verification

    def _invoke_backfill_reprocessing(
        self,
        sfn_client,
        missing_15m: List[Dict[str, str]],
        chunk_index: int,
        chunk_hash: str,
        target: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Invoke backfill Step Functions execution for required 15m windows."""
        state_machine_arn = target.get("resolved_target")
        if not state_machine_arn:
            raise ValueError("Backfill remediation branch required but no Step Functions target was resolved")

        start_ts = min(window["start_ts_iso"] for window in missing_15m)
        end_ts = max(window["end_ts_iso"] for window in missing_15m)
        name = f"if-remediate-{self.runtime_config.run_id}-c{chunk_index}-{chunk_hash}"[:80]
        family_ranges = [{"start_ts": window["start_ts_iso"], "end_ts": window["end_ts_iso"]} for window in missing_15m]
        planner = build_family_range_plan(
            family_ranges={"delta": family_ranges, "fg_a": family_ranges, "pair_counts": family_ranges, "fg_c": family_ranges},
            requested_families=["delta", "fg_a", "pair_counts", "fg_c"],
        )
        payload = {
            "project_name": self.runtime_config.project_name,
            "feature_spec_version": self.runtime_config.feature_spec_version,
            "ml_project_name": self.runtime_config.ml_project_name,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "source": "if_training_remediation",
            "run_id": self.runtime_config.run_id,
            "chunk_index": chunk_index,
            "chunk_hash": chunk_hash,
            "ranges": [{"start_ts_iso": item["start_ts_iso"], "end_ts_iso": item["end_ts_iso"]} for item in missing_15m],
            "manifest": build_execution_manifest(
                project_name=self.runtime_config.project_name,
                feature_spec_version=self.runtime_config.feature_spec_version,
                planner_mode="caller_guided",
                source="if_training_remediation",
                family_plan=planner,
                run_id=self.runtime_config.run_id,
            ),
        }
        try:
            response = sfn_client.start_execution(
                stateMachineArn=state_machine_arn,
                name=name,
                input=json.dumps(payload),
            )
        except Exception as exc:  # pylint: disable=broad-except
            if "ExecutionAlreadyExists" in str(exc):
                return {
                    "status": "Skipped",
                    "execution_name": name,
                    "chunk_index": chunk_index,
                    "chunk_hash": chunk_hash,
                    "requested_range": {"start_ts": start_ts, "end_ts": end_ts},
                    "failure_reason": "ExecutionAlreadyExists",
                }
            raise
        final = self._wait_for_stepfunctions_execution(sfn_client, response["executionArn"])
        return {
            "status": final["status"],
            "execution_arn": response["executionArn"],
            "chunk_index": chunk_index,
            "chunk_hash": chunk_hash,
            "requested_range": {"start_ts": start_ts, "end_ts": end_ts},
            "failure_reason": final.get("cause") or final.get("error"),
            "target_resolution": target,
        }

    def _invoke_fgb_baseline_rebuild(
        self,
        sagemaker_client,
        missing_fgb: List[Dict[str, Any]],
        chunk_index: int,
        chunk_hash: str,
        target: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Invoke FG-B baseline pipeline for missing baseline references."""
        pipeline_name = target.get("resolved_target")
        if not pipeline_name:
            raise ValueError("FG-B remediation branch required but no pipeline target was resolved")
        references = sorted({entry["start_ts_iso"] for entry in missing_fgb})
        runs: List[Dict[str, Any]] = []
        for idx, reference_time_iso in enumerate(references, start=1):
            params = [
                {"Name": "ProjectName", "Value": self.runtime_config.project_name},
                {"Name": "FeatureSpecVersion", "Value": self.runtime_config.feature_spec_version},
                {"Name": "ReferenceTimeIso", "Value": reference_time_iso},
                {"Name": "Mode", "Value": "baseline"},
            ]
            started = sagemaker_client.start_pipeline_execution(
                PipelineName=pipeline_name,
                PipelineExecutionDescription=f"if_training remediation run_id={self.runtime_config.run_id} chunk={chunk_index} hash={chunk_hash} reference={reference_time_iso} idx={idx}",
                PipelineParameters=params,
            )
            final = self._wait_for_pipeline_execution(sagemaker_client, started["PipelineExecutionArn"])
            runs.append(
                {
                    "reference_time_iso": reference_time_iso,
                    "pipeline_execution_arn": started["PipelineExecutionArn"],
                    "status": final["status"],
                    "failure_reason": final.get("failure_reason", ""),
                    "chunk_index": chunk_index,
                    "chunk_hash": chunk_hash,
                }
            )
        status = "Succeeded" if runs and all(item["status"] == "Succeeded" for item in runs) else ("Skipped" if not runs else "Failed")
        return {"status": status, "pipeline_name": pipeline_name, "executions": runs, "target_resolution": target}

    def _wait_for_stepfunctions_execution(self, sfn_client, execution_arn: str, max_polls: int = 120, interval_seconds: int = 10) -> Dict[str, Any]:
        """Poll stepfunctions execution until terminal status."""
        for _ in range(max_polls):
            response = sfn_client.describe_execution(executionArn=execution_arn)
            status = response.get("status", "RUNNING")
            if status not in {"RUNNING"}:
                return {
                    "status": "Succeeded" if status == "SUCCEEDED" else status,
                    "error": response.get("error", ""),
                    "cause": response.get("cause", ""),
                }
            time.sleep(interval_seconds)
        raise TimeoutError(f"StepFunctions execution did not complete in time: {execution_arn}")

    def _wait_for_pipeline_execution(self, sagemaker_client, execution_arn: str, max_polls: int = 120, interval_seconds: int = 15) -> Dict[str, Any]:
        """Poll SageMaker pipeline execution until terminal status."""
        for _ in range(max_polls):
            response = sagemaker_client.describe_pipeline_execution(PipelineExecutionArn=execution_arn)
            status = response.get("PipelineExecutionStatus", "Executing")
            if status not in {"Executing", "Stopping"}:
                return {
                    "status": status,
                    "failure_reason": response.get("FailureReason", ""),
                }
            time.sleep(interval_seconds)
        raise TimeoutError(f"Pipeline execution did not complete in time: {execution_arn}")

    def _run_publish_stage(self) -> None:
        """Persist deterministic model publication metadata."""
        report = self._load_final_training_report()
        publication = {
            "run_id": self.runtime_config.run_id,
            "project_name": self.runtime_config.project_name,
            "ml_project_name": self.runtime_config.ml_project_name,
            "feature_spec_version": self.runtime_config.feature_spec_version,
            "model_version": self.training_spec.model_version,
            "published_at": self.runtime_config.execution_ts_iso,
            "model_tar_s3_uri": report["final_model"]["model_image_copy_path"],
            "model_hash": report["final_model"].get("artifact_hash"),
            "validation_gates": report.get("validation_gates", {}),
            "status": "published",
        }
        self._write_stage_status("publish", publication)

    def _run_attributes_stage(self) -> None:
        """Persist deterministic model attributes metadata."""
        publish = self._load_stage_status("publish")
        attributes = {
            "run_id": self.runtime_config.run_id,
            "model_version": publish.get("model_version", self.training_spec.model_version),
            "feature_spec_version": self.runtime_config.feature_spec_version,
            "attributes": {
                "join_keys": self.training_spec.join_keys,
                "window": asdict(self.training_spec.window),
                "preprocessing": asdict(self.training_spec.preprocessing),
            },
            "status": "registered",
        }
        self._write_stage_status("attributes", attributes)

    def _run_deploy_stage(self) -> None:
        """Deploy model from publication metadata and persist rollout status."""
        publish = self._load_stage_status("publish")
        gates = publish.get("validation_gates", {})
        normalized_gates = {
            name: value if isinstance(value, dict) else {"passed": bool(value)}
            for name, value in gates.items()
        }
        if not normalized_gates:
            normalized_gates = {
                "min_relative_improvement": {"passed": True},
                "max_alert_volume_delta": {"passed": True},
                "max_score_drift": {"passed": True},
            }
        deploy_status = self._maybe_deploy(
            model_data_url=publish["model_tar_s3_uri"],
            gates=normalized_gates,
        )
        self._write_stage_status(
            "deploy",
            {
                "run_id": self.runtime_config.run_id,
                "model_tar_s3_uri": publish["model_tar_s3_uri"],
                "gates": normalized_gates,
                "deployment_status": deploy_status,
            },
        )

    def _extract_missing_windows_manifest(
        self,
        *,
        preflight: Dict[str, Any],
        train_start: datetime,
        train_end: datetime,
        evaluation_windows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Extract canonical missing-window manifest using Batch Index as authority."""
        baseline_start = train_start
        baseline_end = train_end
        if evaluation_windows:
            baseline_start = min([train_start, *[self._parse_iso_ts(window["start_ts"]) for window in evaluation_windows]])
            baseline_end = max([train_end, *[self._parse_iso_ts(window["end_ts"]) for window in evaluation_windows]])
        readiness = self._derive_batch_index_readiness(
            required_start=train_start,
            required_end=train_end,
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            preflight=preflight,
        )
        return readiness["missing_windows_manifest"]

    @staticmethod
    def _floor_to_15m(ts: datetime) -> datetime:
        minute = (ts.minute // 15) * 15
        return ts.replace(minute=minute, second=0, microsecond=0)

    def _derive_batch_index_readiness(
        self,
        *,
        required_start: datetime,
        required_end: datetime,
        baseline_start: datetime,
        baseline_end: datetime,
        preflight: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        """Derive expected/observed/unresolved ranges from Batch Index evidence."""
        from ndr.config.batch_index_loader import BatchIndexLoader

        required_start_iso = required_start.isoformat().replace("+00:00", "Z")
        required_end_iso = required_end.isoformat().replace("+00:00", "Z")
        loader = BatchIndexLoader(table_name=self.runtime_config.batch_index_table_name)
        rows = loader.lookup_forward(
            project_name=self.runtime_config.project_name,
            data_source_name=self.runtime_config.project_name,
            version=self.runtime_config.feature_spec_version,
            start_ts_iso=required_start_iso,
            end_ts_iso=required_end_iso,
        )
        scoped_rows = [
            row for row in rows
            if not row.ml_project_names or self.runtime_config.ml_project_name in row.ml_project_names
        ]
        observed_15m = {self._floor_to_15m(self._parse_iso_ts(row.etl_ts)).isoformat().replace("+00:00", "Z") for row in scoped_rows}
        expected_15m: List[Dict[str, str]] = []
        cursor = self._floor_to_15m(required_start)
        while cursor < required_end:
            expected_15m.append(
                {
                    "start_ts_iso": cursor.isoformat().replace("+00:00", "Z"),
                    "end_ts_iso": (cursor + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
                }
            )
            cursor += timedelta(minutes=15)
        unresolved_15m = [window for window in expected_15m if window["start_ts_iso"] not in observed_15m]

        expected_days = sorted(
            (baseline_start.date() + timedelta(days=offset)).isoformat()
            for offset in range(max((baseline_end.date() - baseline_start.date()).days, 1))
        )
        observed_days = sorted({row.date_partition.replace("/", "-") for row in scoped_rows})
        unresolved_days = [day for day in expected_days if day not in observed_days]

        if isinstance(preflight, dict):
            partition_coverage = preflight.get("partition_coverage", {})
            preflight_missing: set[str] = set()
            for dataset in ("fg_a", "fg_c"):
                details = partition_coverage.get(dataset) or {}
                preflight_missing.update(details.get("missing_partitions", []))
            if preflight_missing and not unresolved_days:
                raise ValueError(
                    "Contract decision required: Batch Index indicates complete coverage while feature-store verification reports missing partitions."
                )

        missing_windows_manifest = from_missing_sources(
            missing_15m_windows=[{"window_start_ts": item["start_ts_iso"], "window_end_ts": item["end_ts_iso"]} for item in unresolved_15m],
            missing_fgb_windows=[{"reference_time_iso": f"{day}T00:00:00Z", "horizons": ["7d", "30d"]} for day in unresolved_days],
            project_name=self.runtime_config.project_name,
            feature_spec_version=self.runtime_config.feature_spec_version,
            ml_project_name=self.runtime_config.ml_project_name,
            run_id=self.runtime_config.run_id,
        )
        readiness_manifest = {
            "contract_version": "training_readiness_manifest.v1",
            "run_id": self.runtime_config.run_id,
            "project_name": self.runtime_config.project_name,
            "ml_project_name": self.runtime_config.ml_project_name,
            "feature_spec_version": self.runtime_config.feature_spec_version,
            "batch_index_evidence": {
                "table_name": self.runtime_config.batch_index_table_name,
                "selectors": {
                    "pk": self.runtime_config.project_name,
                    "range_start_ts_iso": required_start_iso,
                    "range_end_ts_iso": required_end_iso,
                },
                "observed_items": [
                    {"pk": row.project_name, "sk": row.batch_id, "etl_ts": row.etl_ts, "date_partition": row.date_partition}
                    for row in scoped_rows
                ],
            },
            "families": {
                "fg_a_15m": {
                    "expected_ranges": expected_15m,
                    "observed_window_starts": sorted(observed_15m),
                    "unresolved_ranges": unresolved_15m,
                },
                "fg_b_daily": {
                    "expected_days": expected_days,
                    "observed_days": observed_days,
                    "unresolved_days": unresolved_days,
                },
            },
        }
        return {
            "training_readiness_manifest": readiness_manifest,
            "missing_windows_manifest": missing_windows_manifest,
        }

    def _load_latest_verification_status(self) -> Dict[str, Any]:
        """Load verification status persisted by verify stage."""
        return self._load_stage_status(
            "verification/latest_status",
            default={
                "needs_remediation": False,
                "missing_windows_manifest": from_missing_sources(
                    missing_15m_windows=[],
                    missing_fgb_windows=[],
                    project_name=self.runtime_config.project_name,
                    feature_spec_version=self.runtime_config.feature_spec_version,
                    ml_project_name=self.runtime_config.ml_project_name,
                    run_id=self.runtime_config.run_id,
                ),
            },
        )

    def _load_final_training_report(self) -> Dict[str, Any]:
        """Load final training report artifact produced by train stage."""
        return self._load_stage_status("final_training_report", base_dir="", filename="final_training_report.json")

    def _write_stage_status(self, stage: str, payload: Dict[str, Any]) -> None:
        """Write a stage status payload to report storage for observability."""
        import boto3

        bucket, key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        key = f"{self._report_run_prefix(key_prefix)}/{stage}.json"
        client = boto3.client("s3")
        _put_json(client, bucket, key, payload)

    def _run_post_training_evaluation(self, evaluation_windows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Emit per-window evaluation replay/join artifacts using reuse-first orchestration metadata."""
        import boto3

        if not self._resolve_toggle(
            self.training_spec.toggles.enable_post_training_evaluation,
        ):
            return []

        bucket, key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        client = boto3.client("s3")
        sagemaker = boto3.client("sagemaker")
        dependency_readiness = self._run_dependency_readiness_gate(stage="evaluation")
        inference_target = next((c for c in dependency_readiness["checks"] if c["family"] == "inference"), {})
        join_target = next((c for c in dependency_readiness["checks"] if c["family"] == "prediction_feature_join"), {})
        inference_pipeline_name = inference_target.get("resolved_target")
        join_pipeline_name = join_target.get("resolved_target")
        manifests: List[Dict[str, Any]] = []
        for window in evaluation_windows:
            window_id = window["window_id"]
            base = f"{self._report_run_prefix(key_prefix)}/evaluation/{window_id}"
            inference_run = self._invoke_evaluation_pipeline(
                sagemaker_client=sagemaker,
                pipeline_name=inference_pipeline_name,
                window_id=window_id,
                start_ts=window["start_ts"],
                end_ts=window["end_ts"],
                mode="inference",
            )
            join_publication_enabled = self._resolve_toggle(
                self.training_spec.toggles.enable_eval_join_publication,
            )
            if join_publication_enabled:
                join_run = self._invoke_evaluation_pipeline(
                    sagemaker_client=sagemaker,
                    pipeline_name=join_pipeline_name,
                    window_id=window_id,
                    start_ts=window["start_ts"],
                    end_ts=window["end_ts"],
                    mode="join",
                )
            else:
                join_run = {
                    "status": "Skipped",
                    "pipeline_execution_arn": "",
                    "failure_reason": "EnableEvalJoinPublication disabled",
                }

            records_scored = self._count_inference_records_for_window(
                start_ts=window["start_ts"],
                end_ts=window["end_ts"],
            )
            predictions = {
                "window_id": window_id,
                "orchestrator": inference_pipeline_name,
                "start_ts": window["start_ts"],
                "end_ts": window["end_ts"],
                "status": inference_run["status"],
                "pipeline_execution_arn": inference_run["pipeline_execution_arn"],
                "failure_reason": inference_run.get("failure_reason", ""),
                "target_resolution": inference_target,
            }
            join_manifest = {
                "window_id": window_id,
                "orchestrator": join_pipeline_name,
                "publish_enabled": join_publication_enabled,
                "status": join_run["status"],
                "pipeline_execution_arn": join_run["pipeline_execution_arn"],
                "failure_reason": join_run.get("failure_reason", ""),
                "target_resolution": join_target,
            }
            metrics = {
                "window_id": window_id,
                "status": "Succeeded" if inference_run["status"] == "Succeeded" and join_run["status"] in {"Succeeded", "Skipped"} else "Failed",
                "records_scored": records_scored,
                "inference_pipeline_status": inference_run["status"],
                "join_pipeline_status": join_run["status"],
            }
            _put_json(client, bucket, f"{base}/predictions_manifest.json", predictions)
            _put_json(client, bucket, f"{base}/join_manifest.json", join_manifest)
            _put_json(client, bucket, f"{base}/metrics.json", metrics)
            manifests.append({"window_id": window_id, "predictions_manifest": predictions, "join_manifest": join_manifest, "metrics": metrics})
        return manifests

    def _invoke_evaluation_pipeline(
        self,
        sagemaker_client,
        pipeline_name: str,
        window_id: str,
        start_ts: str,
        end_ts: str,
        mode: str,
    ) -> Dict[str, Any]:
        """Invoke evaluation pipeline execution and wait for completion."""
        params = [
            {"Name": "ProjectName", "Value": self.runtime_config.project_name},
            {"Name": "FeatureSpecVersion", "Value": self.runtime_config.feature_spec_version},
            {"Name": "MlProjectName", "Value": self.runtime_config.ml_project_name},
            {"Name": "MiniBatchId", "Value": f"eval-{self.runtime_config.run_id}-{window_id}"},
            {"Name": "BatchStartTsIso", "Value": start_ts},
            {"Name": "BatchEndTsIso", "Value": end_ts},
        ]
        started = sagemaker_client.start_pipeline_execution(
            PipelineName=pipeline_name,
            PipelineExecutionDescription=f"if_training {mode} evaluation run_id={self.runtime_config.run_id} window={window_id}",
            PipelineParameters=params,
        )
        final = self._wait_for_pipeline_execution(sagemaker_client, started["PipelineExecutionArn"])
        return {
            "status": final["status"],
            "pipeline_execution_arn": started["PipelineExecutionArn"],
            "failure_reason": final.get("failure_reason", ""),
        }

    def _count_inference_records_for_window(self, start_ts: str, end_ts: str) -> int:
        """Best-effort count of persisted inference records for one evaluation window."""
        from ndr.config.job_spec_loader import load_job_spec
        from pyspark.sql import functions as F

        try:
            inference_spec = load_job_spec(
                project_name=self.runtime_config.project_name,
                job_name="inference_predictions",
                feature_spec_version=self.runtime_config.feature_spec_version,
            )
            output = inference_spec.get("output") or {}
            s3_prefix = output.get("s3_prefix")
            if not s3_prefix:
                return 0
            start_dt = self._parse_iso_ts(start_ts)
            end_dt = self._parse_iso_ts(end_ts)
            df = self.spark.read.option("mergeSchema", "true").parquet(s3_prefix)
            if "window_end_ts" not in df.columns:
                return 0
            return int(
                df.where((F.col("window_end_ts") >= F.lit(start_dt)) & (F.col("window_end_ts") < F.lit(end_dt))).count()
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Unable to compute evaluation records_scored: %s", exc)
            return 0

    def _load_stage_status(
        self,
        stage: str,
        default: Dict[str, Any] | None = None,
        base_dir: str = "",
        filename: str | None = None,
    ) -> Dict[str, Any]:
        """Read a stage status payload from report storage."""
        import boto3

        bucket, key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        if filename:
            suffix = filename
        else:
            suffix = f"{stage}.json"
        stage_prefix = f"{base_dir.strip('/')}/" if base_dir else ""
        key = f"{self._report_run_prefix(key_prefix)}/{stage_prefix}{suffix}"
        client = boto3.client("s3")
        try:
            response = client.get_object(Bucket=bucket, Key=key)
        except Exception:  # pylint: disable=broad-except
            if default is not None:
                return default
            raise
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)

    def run(self) -> None:
        """Execute the full workflow for this job runner."""
        self._validate_runtime_contract()
        lifecycle_stage = (self.runtime_config.stage or "train").lower()
        if lifecycle_stage in {"verify", "reverify"}:
            self._run_verification_stage(lifecycle_stage)
            return
        if lifecycle_stage == "plan":
            self._run_planning_stage()
            return
        if lifecycle_stage == "remediate":
            self._run_remediation_stage()
            return
        if lifecycle_stage == "publish":
            self._run_publish_stage()
            return
        if lifecycle_stage == "attributes":
            self._run_attributes_stage()
            return
        if lifecycle_stage == "deploy":
            self._run_deploy_stage()
            return

        train_start, train_end = self._resolve_training_window()
        evaluation_windows = self._resolve_evaluation_windows()
        self._enforce_pre_train_reverify_gate()
        history_plan = self._load_required_history_plan()

        planned_entries = ensure_manifest((history_plan or {}).get("missing_windows_manifest"))["entries"]
        missing_15m_manifest = next((entry["ranges"] for entry in planned_entries if entry["artifact_family"] == "fg_a_15m"), [])
        missing_fgb_manifest = next((entry["ranges"] for entry in planned_entries if entry["artifact_family"] == "fg_b_daily"), [])
        self._run_dependency_readiness_gate(
            stage="train",
            missing_15m_manifest=missing_15m_manifest,
            missing_fgb_manifest=missing_fgb_manifest,
        )
        stage = "read_inputs"
        try:
            fg_a_df = self._read_windowed_input("fg_a", train_start, train_end)
            fg_c_df = self._read_windowed_input("fg_c", train_start, train_end)

            stage = "schema_enforcement"
            fg_a_df = enforce_schema(fg_a_df, build_fg_a_manifest(), "fg_a", logger)
            fg_c_df = enforce_schema(fg_c_df, build_fg_c_manifest(metrics=build_fg_c_metric_names()), "fg_c", logger)

            stage = "preflight"
            preflight = self._preflight_validation(fg_a_df, fg_c_df, train_start, train_end)
            stage = "join"
            joined_df = fg_a_df.join(fg_c_df, on=self.training_spec.join_keys, how="inner")

            metadata_columns, feature_columns = split_metadata_and_feature_columns(
                joined_df.columns,
                self.training_spec.join_keys,
            )
            feature_columns = [
                c for c in feature_columns
                if c not in {"record_id", "mini_batch_id", "window_start_ts", "hour_of_day", "day_of_week"}
            ]
            if not feature_columns:
                raise ValueError("No numeric feature columns available for IF training")

            split_ts = train_end - timedelta(days=30)
            train_df = joined_df.filter(joined_df.window_end_ts < split_ts)
            val_df = joined_df.filter(joined_df.window_end_ts >= split_ts)
            if train_df.rdd.isEmpty() or val_df.rdd.isEmpty():
                raise ValueError("Training or validation split is empty; unable to tune/train model")

            stage = "preprocessing_tune"
            tune_scaler, tune_outlier = self._fit_preprocessing_params(train_df, feature_columns)
            train_processed = self._apply_preprocessing_with_params(train_df, feature_columns, tune_scaler, tune_outlier)
            val_processed = self._apply_preprocessing_with_params(val_df, feature_columns, tune_scaler, tune_outlier)

            stage = "feature_selection"
            selected_features, selection_meta = self._select_features(
                train_processed=train_processed,
                val_processed=val_processed,
                feature_columns=feature_columns,
            )
            if not selected_features:
                raise ValueError("Feature selection produced an empty feature mask")

            stage = "tuning"
            tuning_summary, best_params, gates, val_metrics = self._tune_and_validate(
                train_processed=train_processed,
                val_processed=val_processed,
                selected_features=selected_features,
            )

            stage = "final_refit"
            final_scaler, final_outlier = self._fit_preprocessing_params(joined_df, feature_columns)
            full_processed = self._apply_preprocessing_with_params(joined_df, feature_columns, final_scaler, final_outlier)
            final_model = self._fit_final_model(full_processed, selected_features, best_params)

            metrics = self._build_metrics(
                full_processed=full_processed,
                train_processed=train_processed,
                val_processed=val_processed,
                all_features=feature_columns,
                selected_features=selected_features,
                tuning_summary=tuning_summary,
                gates=gates,
                val_metrics=val_metrics,
            )

            stage = "persist_artifacts"
            artifact_ctx = self._persist_artifacts(
                scaler_params=final_scaler,
                outlier_params=final_outlier,
                feature_mask=selected_features,
                metrics=metrics,
                tuning_summary=tuning_summary,
                model=final_model,
                train_start=train_start,
                train_end=train_end,
            )

            stage = "deploy"
            deployment_status = self._maybe_deploy(
                model_data_url=artifact_ctx["model_tar_s3_uri"],
                gates=gates,
            )

            latest_model_path = None
            if all(g["passed"] for g in gates.values()):
                latest_model_path = self._promote_latest_model_pointer(artifact_ctx)

            stage = "final_report"
            evaluation_manifests = self._run_post_training_evaluation(evaluation_windows)
            report_s3_uri = self._write_final_report_and_success(
                train_start=train_start,
                train_end=train_end,
                preflight=preflight,
                metadata_columns=metadata_columns,
                selected_features=selected_features,
                scaler_params=final_scaler,
                outlier_params=final_outlier,
                tuning_summary=tuning_summary,
                metrics=metrics,
                gates=gates,
                deployment_status=deployment_status,
                selection_meta=selection_meta,
                artifact_ctx=artifact_ctx,
                latest_model_path=latest_model_path,
                history_planner=history_plan,
                evaluation_manifests=evaluation_manifests,
            )

            stage = "experiment_tracking"
            self._log_sagemaker_experiments(
                metrics=metrics,
                tuning_summary=tuning_summary,
                deployment_status=deployment_status,
                preflight=preflight,
                report_s3_uri=report_s3_uri,
                selected_feature_count=len(selected_features),
                all_feature_count=len(feature_columns),
                history_planner=history_plan,
                evaluation_windows=evaluation_windows,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self._write_failure_report(
                stage=stage,
                error=str(exc),
                train_start=train_start,
                train_end=train_end,
            )
            self._write_failure_experiment_artifact(
                stage=stage,
                error=str(exc),
                train_start=train_start,
                train_end=train_end,
            )
            raise

    def _resolve_training_window(self) -> Tuple[datetime, datetime]:
        """Resolve training window using runtime override or spec-derived defaults."""
        if self.runtime_config.training_start_ts and self.runtime_config.training_end_ts:
            train_start = datetime.fromisoformat(self.runtime_config.training_start_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
            train_end = datetime.fromisoformat(self.runtime_config.training_end_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
            if train_start >= train_end:
                raise ValueError("training_start_ts must be earlier than training_end_ts")
            return train_start, train_end

        exec_ts = self.runtime_config.execution_ts_iso.replace("Z", "+00:00")
        t0 = datetime.fromisoformat(exec_ts).astimezone(timezone.utc)
        month = t0.month - self.training_spec.window.gap_months
        year = t0.year
        while month <= 0:
            month += 12
            year -= 1
        train_end = t0.replace(year=year, month=month)

        month = train_end.month - (self.training_spec.window.lookback_months - self.training_spec.window.gap_months)
        year = train_end.year
        while month <= 0:
            month += 12
            year -= 1
        train_start = train_end.replace(year=year, month=month)
        return train_start, train_end

    def _read_windowed_input(self, name: str, train_start: datetime, train_end: datetime):
        """Execute the read windowed input stage of the workflow."""
        from pyspark.sql import functions as F

        input_spec = self.training_spec.feature_inputs[name]
        logger.info("Reading %s from %s", name, input_spec.s3_prefix)
        df = self.spark.read.option("mergeSchema", "true").parquet(input_spec.s3_prefix)
        if "window_end_ts" not in df.columns:
            raise ValueError(f"{name} input must include window_end_ts")
        return df.filter((F.col("window_end_ts") >= F.lit(train_start)) & (F.col("window_end_ts") < F.lit(train_end)))

    def _preflight_validation(self, fg_a_df, fg_c_df, train_start: datetime, train_end: datetime) -> Dict[str, Any]:
        """Execute the preflight validation stage of the workflow."""
        required_join_keys = self.training_spec.join_keys
        reliability = self.training_spec.reliability

        from pyspark.sql import functions as F

        expected_dates = {
            (train_start.date() + timedelta(days=offset)).isoformat()
            for offset in range(max((train_end.date() - train_start.date()).days, 1))
        }

        counts: Dict[str, int] = {}
        partition_details: Dict[str, Dict[str, Any]] = {}
        window_details: Dict[str, Dict[str, int]] = {}
        join_key_duplication: Dict[str, Dict[str, float]] = {}

        def _fail_preflight(reason: str, payload: Dict[str, Any]) -> None:
            """Execute the fail preflight stage of the workflow."""
            self._write_preflight_failure_artifact(train_start=train_start, train_end=train_end, reason=reason, context=payload)
            raise ValueError(reason)

        for name, df in (("fg_a", fg_a_df), ("fg_c", fg_c_df)):
            missing = [key for key in required_join_keys if key not in df.columns]
            if missing:
                _fail_preflight(
                    f"{name} missing required join keys: {missing}",
                    {"dataset": name, "missing_join_keys": missing},
                )

            row_count = df.count()
            counts[name] = row_count
            if row_count < reliability.min_rows_per_input:
                _fail_preflight(
                    f"{name} row count {row_count} below minimum {reliability.min_rows_per_input}",
                    {"dataset": name, "row_count": row_count, "minimum": reliability.min_rows_per_input},
                )

            distinct_join_keys = df.select(*required_join_keys).distinct().count()
            duplication_ratio = row_count / max(distinct_join_keys, 1)
            join_key_duplication[name] = {
                "rows": float(row_count),
                "distinct_join_keys": float(distinct_join_keys),
                "rows_per_join_key": float(duplication_ratio),
            }
            if duplication_ratio > reliability.max_rows_per_join_key:
                _fail_preflight(
                    f"{name} rows-per-join-key {duplication_ratio:.4f} above maximum {reliability.max_rows_per_join_key:.4f}",
                    {
                        "dataset": name,
                        "rows_per_join_key": duplication_ratio,
                        "maximum_ratio": reliability.max_rows_per_join_key,
                        "distinct_join_keys": distinct_join_keys,
                    },
                )

            if "window_label" in df.columns:
                window_counts = {
                    str(row["window_label"]): int(row["count"])
                    for row in df.groupBy("window_label").count().collect()
                }
                window_details[name] = window_counts
                underfilled = [
                    {"window_label": label, "row_count": count}
                    for label, count in window_counts.items()
                    if count < reliability.min_rows_per_window
                ]
                if underfilled:
                    _fail_preflight(
                        f"{name} window_label coverage below minimum rows {reliability.min_rows_per_window}",
                        {
                            "dataset": name,
                            "minimum_rows_per_window": reliability.min_rows_per_window,
                            "underfilled_windows": underfilled,
                        },
                    )

            dt_frame = df.select(F.to_date(F.col("dt")).alias("dt")) if "dt" in df.columns else df.select(F.to_date(F.col("window_end_ts")).alias("dt"))
            observed_dates = {
                row[0].isoformat()
                for row in dt_frame.where(F.col("dt").isNotNull()).distinct().collect()
            }
            missing_partitions = sorted(expected_dates - observed_dates)
            observed_days = len(observed_dates)
            expected_days = len(expected_dates)
            coverage = observed_days / max(expected_days, 1)
            partition_details[name] = {
                "expected_days": expected_days,
                "observed_days": observed_days,
                "coverage_ratio": coverage,
                "missing_partition_days": len(missing_partitions),
                "missing_partitions": missing_partitions,
            }
            if coverage < reliability.min_partition_coverage_ratio:
                _fail_preflight(
                    f"{name} partition coverage {coverage:.4f} below minimum {reliability.min_partition_coverage_ratio:.4f}",
                    {
                        "dataset": name,
                        "partition_coverage": partition_details[name],
                        "minimum_ratio": reliability.min_partition_coverage_ratio,
                    },
                )

        if window_details.get("fg_a") and window_details.get("fg_c"):
            common_windows = sorted(set(window_details["fg_a"]).intersection(window_details["fg_c"]))
            if not common_windows:
                _fail_preflight(
                    "No overlapping window_label horizons between fg_a and fg_c",
                    {"fg_a_windows": sorted(window_details["fg_a"].keys()), "fg_c_windows": sorted(window_details["fg_c"].keys())},
                )

            underfilled_overlap = [
                {
                    "window_label": window,
                    "fg_a_rows": window_details["fg_a"].get(window, 0),
                    "fg_c_rows": window_details["fg_c"].get(window, 0),
                }
                for window in common_windows
                if min(window_details["fg_a"].get(window, 0), window_details["fg_c"].get(window, 0)) < reliability.min_rows_per_window
            ]
            if underfilled_overlap:
                _fail_preflight(
                    f"Overlapping windows below minimum rows {reliability.min_rows_per_window}",
                    {
                        "minimum_rows_per_window": reliability.min_rows_per_window,
                        "underfilled_overlapping_windows": underfilled_overlap,
                    },
                )

        fg_a_keys = fg_a_df.select(*required_join_keys).distinct()
        fg_c_keys = fg_c_df.select(*required_join_keys).distinct()
        join_keys_count = fg_a_keys.join(fg_c_keys, on=required_join_keys, how="inner").count()
        if join_keys_count < reliability.min_join_rows:
            _fail_preflight(
                f"join row count {join_keys_count} below minimum {reliability.min_join_rows}",
                {"join_key_count": join_keys_count, "minimum": reliability.min_join_rows},
            )

        coverage_ratio = join_keys_count / max(min(counts["fg_a"], counts["fg_c"]), 1)
        if coverage_ratio < reliability.min_join_coverage_ratio:
            _fail_preflight(
                f"join coverage ratio {coverage_ratio:.4f} below minimum {reliability.min_join_coverage_ratio:.4f}",
                {"join_coverage_ratio": coverage_ratio, "minimum_ratio": reliability.min_join_coverage_ratio},
            )

        return {
            "input_counts": counts,
            "join_key_count": join_keys_count,
            "join_coverage_ratio": coverage_ratio,
            "partition_coverage": partition_details,
            "window_coverage": window_details,
            "join_key_duplication": join_key_duplication,
            "expected_days": len(expected_dates),
            "join_keys": required_join_keys,
        }

    def _fit_preprocessing_params(self, df, feature_columns: List[str]) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
        """Execute the fit preprocessing params stage of the workflow."""
        from pyspark.sql import functions as F

        eps = self.training_spec.preprocessing.eps
        z_max = self.training_spec.preprocessing.z_max
        scaler_params: Dict[str, Dict[str, float]] = {}
        outlier_params: Dict[str, Dict[str, float]] = {}

        for feature in feature_columns:
            q = df.approxQuantile(feature, [0.25, 0.5, 0.75], 0.001)
            q = q if q else [0.0, 0.0, 0.0]
            median = float(q[1])
            iqr = float(q[2] - q[0])
            mad_df = df.select(F.abs(F.col(feature) - F.lit(median)).alias("mad_col"))
            mad_q = mad_df.approxQuantile("mad_col", [0.5], 0.001)
            mad = float(mad_q[0]) if mad_q else 0.0
            scaler_params[feature] = {"median": median, "iqr": iqr, "eps": eps}
            outlier_params[feature] = {"median": median, "mad": mad, "z_max": z_max, "eps": eps}

        return scaler_params, outlier_params

    def _apply_preprocessing_with_params(self, df, feature_columns: List[str], scaler_params, outlier_params):
        """Execute the apply preprocessing with params stage of the workflow."""
        from pyspark.sql import functions as F

        out = df
        for feature in feature_columns:
            scaler = scaler_params.get(feature, {})
            outlier = outlier_params.get(feature, {})
            median = float(scaler.get("median", outlier.get("median", 0.0)))
            iqr = float(scaler.get("iqr", 1.0))
            mad = float(outlier.get("mad", 1.0))
            eps = float(scaler.get("eps", outlier.get("eps", self.training_spec.preprocessing.eps)))
            z_max = float(outlier.get("z_max", self.training_spec.preprocessing.z_max))
            robust_z = (F.col(feature) - F.lit(median)) / (F.lit(mad) + F.lit(eps))
            clipped = F.when(robust_z > z_max, F.lit(z_max)).when(robust_z < -z_max, F.lit(-z_max)).otherwise(robust_z)
            scaled = (F.col(feature) - F.lit(median)) / (F.lit(iqr) + F.lit(eps))

            strategy = (self.training_spec.preprocessing.imputation_strategy or "median_scaled").lower()
            if strategy == "none":
                imputed = F.coalesce(clipped, scaled)
            elif strategy == "constant":
                imputed = F.coalesce(clipped, scaled, F.lit(float(self.training_spec.preprocessing.imputation_constant)))
            else:
                # median_scaled => transformed median is 0.0
                imputed = F.coalesce(clipped, scaled, F.lit(0.0))

            out = out.withColumn(feature, imputed)
        return out

    def _select_features(self, train_processed, val_processed, feature_columns: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        """Execute the select features stage of the workflow."""
        from pyspark.sql import functions as F
        import numpy as np
        from sklearn.ensemble import IsolationForest

        if not self.training_spec.feature_selection.enabled:
            return feature_columns, {"steps": ["disabled"]}

        selected: List[str] = []
        for feature in feature_columns:
            variance = train_processed.select(F.var_pop(F.col(feature)).alias("var")).collect()[0]["var"]
            if variance is not None and float(variance) > self.training_spec.feature_selection.variance_threshold:
                selected.append(feature)

        pruned: List[str] = []
        for feature in selected:
            if not pruned:
                pruned.append(feature)
                continue
            max_corr = max(abs(train_processed.stat.corr(feature, kept) or 0.0) for kept in pruned)
            if max_corr <= self.training_spec.feature_selection.corr_threshold:
                pruned.append(feature)

        # Stability selection using bootstrap frequency of score-correlation signal.
        train_pdf = train_processed.select(*pruned).toPandas().fillna(0.0)
        stability_counts = {f: 0 for f in pruned}
        stability_runs = min(10, max(3, self.training_spec.tuning.max_trials // 2))
        if not train_pdf.empty and pruned:
            x = train_pdf.to_numpy(dtype=float)
            rng = np.random.default_rng(self.training_spec.random_seed)
            top_k = max(1, int(len(pruned) * 0.5))
            for run_idx in range(stability_runs):
                sample_idx = rng.integers(0, x.shape[0], size=x.shape[0])
                x_boot = x[sample_idx]
                model = IsolationForest(
                    n_estimators=100,
                    max_samples=1.0,
                    max_features=1.0,
                    contamination=0.01,
                    bootstrap=False,
                    random_state=self.training_spec.random_seed + run_idx,
                    n_jobs=1,
                ).fit(x_boot)
                scores = -model.score_samples(x_boot)
                importances = []
                for i, feature in enumerate(pruned):
                    col = x_boot[:, i]
                    if np.std(col) <= 1e-12:
                        importance = 0.0
                    else:
                        importance = abs(float(np.corrcoef(col, scores)[0, 1]))
                        if np.isnan(importance):
                            importance = 0.0
                    importances.append((feature, importance))
                for feat, _ in sorted(importances, key=lambda t: t[1], reverse=True)[:top_k]:
                    stability_counts[feat] += 1

        stability_selected = [
            feat for feat, cnt in stability_counts.items() if cnt / max(stability_runs, 1) >= 0.6
        ] or pruned

        # Permutation proxy on validation month.
        val_pdf = val_processed.select(*stability_selected).toPandas().fillna(0.0)
        permutation_selected = stability_selected
        permutation_impacts: Dict[str, float] = {f: 0.0 for f in stability_selected}
        if not val_pdf.empty and stability_selected:
            x_train = train_processed.select(*stability_selected).toPandas().fillna(0.0).to_numpy(dtype=float)
            x_val = val_pdf.to_numpy(dtype=float)
            ref_model = IsolationForest(
                n_estimators=100,
                max_samples=1.0,
                max_features=1.0,
                contamination=0.01,
                bootstrap=False,
                random_state=self.training_spec.random_seed,
                n_jobs=1,
            ).fit(x_train)
            base_scores = -ref_model.score_samples(x_val)
            rng = np.random.default_rng(self.training_spec.random_seed + 991)
            for i, feature in enumerate(stability_selected):
                perm = x_val.copy()
                perm[:, i] = perm[rng.permutation(perm.shape[0]), i]
                perm_scores = -ref_model.score_samples(perm)
                impact = float(np.mean(np.abs(base_scores - perm_scores)))
                permutation_impacts[feature] = impact
            impacts = list(permutation_impacts.values())
            threshold = np.percentile(impacts, 40) if impacts else 0.0
            permutation_selected = [f for f, imp in permutation_impacts.items() if imp >= threshold] or stability_selected

        meta = {
            "steps": ["near_constant", "correlation_pruning", "stability_selection", "permutation_proxy"],
            "stability_runs": stability_runs,
            "stability_counts": stability_counts,
            "permutation_impacts": permutation_impacts,
        }
        return permutation_selected, meta

    def _tune_and_validate(self, train_processed, val_processed, selected_features: List[str]):
        """Execute the tune and validate stage of the workflow."""
        import numpy as np
        from sklearn.ensemble import IsolationForest
        from ndr.processing.bayesian_search_fallback import run_bayesian_search

        train_pdf = train_processed.select(*selected_features).toPandas().fillna(0.0)
        val_pdf = val_processed.select(*selected_features).toPandas().fillna(0.0)
        if train_pdf.empty or val_pdf.empty:
            raise ValueError("Training or validation dataset became empty after preprocessing")

        x_train = train_pdf.to_numpy(dtype=float)
        x_val = val_pdf.to_numpy(dtype=float)

        search_space = self.training_spec.tuning.search_space or {
            "n_estimators": [200, 400, 600, 800],
            "max_samples": [0.2, 0.4, 0.6, 0.8, 1.0],
            "max_features": [0.3, 0.5, 0.7, 1.0],
            "contamination": [0.001, 0.005, 0.01, 0.02, 0.05],
            "bootstrap": [False, True],
        }

        def eval_params(params: Dict[str, Any]) -> Dict[str, float]:
            """Execute the eval params stage of the workflow."""
            model = IsolationForest(**params, random_state=self.training_spec.random_seed, n_jobs=1).fit(x_train)
            train_scores = -model.score_samples(x_train)
            val_scores = -model.score_samples(x_val)
            train_std = float(np.std(train_scores)) + 1e-6
            drift = abs(float(np.median(val_scores) - np.median(train_scores))) / train_std
            threshold = float(np.quantile(train_scores, 0.95))
            val_alert_rate = float((val_scores > threshold).mean())
            contamination = float(params.get("contamination", 0.01))
            alert_delta = abs(val_alert_rate - contamination)
            return {"objective": float(drift + alert_delta), "drift": float(drift), "alert_delta": float(alert_delta)}

        baseline_params = {
            "n_estimators": 200,
            "max_samples": 1.0,
            "max_features": 1.0,
            "contamination": 0.01,
            "bootstrap": False,
        }
        baseline_eval = eval_params(baseline_params)
        trials: List[Dict[str, Any]] = [{"trial": 0, "params": baseline_params, **baseline_eval}]

        hpo_method = "optuna_tpe"
        hpo_fallback_used = False
        try:
            import optuna

            sampler = optuna.samplers.TPESampler(seed=self.training_spec.random_seed)
            study = optuna.create_study(direction="minimize", sampler=sampler)

            def objective(trial: optuna.trial.Trial) -> float:
                """Execute the objective stage of the workflow."""
                params = {
                    "n_estimators": trial.suggest_categorical("n_estimators", search_space["n_estimators"]),
                    "max_samples": trial.suggest_categorical("max_samples", search_space["max_samples"]),
                    "max_features": trial.suggest_categorical("max_features", search_space["max_features"]),
                    "contamination": trial.suggest_categorical("contamination", search_space["contamination"]),
                    "bootstrap": trial.suggest_categorical("bootstrap", search_space["bootstrap"]),
                }
                score = eval_params(params)
                trial.set_user_attr("params", params)
                trial.set_user_attr("drift", score["drift"])
                trial.set_user_attr("alert_delta", score["alert_delta"])
                return score["objective"]

            study.optimize(
                objective,
                n_trials=max(self.training_spec.tuning.max_trials - 1, 1),
                timeout=self.training_spec.tuning.timeout_seconds,
                gc_after_trial=True,
            )
            for t in study.trials:
                trials.append(
                    {
                        "trial": int(t.number) + 1,
                        "params": t.user_attrs.get("params", {}),
                        "objective": float(t.value),
                        "drift": float(t.user_attrs.get("drift", 0.0)),
                        "alert_delta": float(t.user_attrs.get("alert_delta", 0.0)),
                    }
                )
        except ImportError:
            trials, _ = run_bayesian_search(
                search_space=search_space,
                evaluate_fn=eval_params,
                max_trials=max(self.training_spec.tuning.max_trials, 1),
                seed=self.training_spec.random_seed,
                initial_params=baseline_params,
            )
            hpo_method = "local_bayesian_fallback"
            hpo_fallback_used = True
            logger.warning(
                "Optuna dependency unavailable; using local Bayesian fallback for IF training HPO.",
                extra={
                    "hpo_method": hpo_method,
                    "hpo_fallback_used": hpo_fallback_used,
                    "project_name": self.runtime_config.project_name,
                    "feature_spec_version": self.runtime_config.feature_spec_version,
                    "run_id": self.runtime_config.run_id,
                },
            )

        best_trial = min(trials, key=lambda t: t["objective"])
        baseline_obj = baseline_eval["objective"]
        best_obj = best_trial["objective"]
        rel_improvement = 0.0 if baseline_obj == 0 else (baseline_obj - best_obj) / abs(baseline_obj)

        ref_model = IsolationForest(**best_trial["params"], random_state=self.training_spec.random_seed, n_jobs=1).fit(x_train)
        train_scores = -ref_model.score_samples(x_train)
        val_scores = -ref_model.score_samples(x_val)
        alert_threshold = float(np.quantile(train_scores, 0.95))
        val_alert_rate = float((val_scores > alert_threshold).mean())
        alert_delta = abs(val_alert_rate - 0.05)
        score_drift = abs(float(np.median(val_scores) - np.median(train_scores))) / (float(np.std(train_scores)) + 1e-6)

        gates = {
            "min_relative_improvement": {
                "threshold": self.training_spec.validation_gates.min_relative_improvement,
                "value": rel_improvement,
                "passed": rel_improvement >= self.training_spec.validation_gates.min_relative_improvement,
            },
            "max_alert_volume_delta": {
                "threshold": self.training_spec.validation_gates.max_alert_volume_delta,
                "value": alert_delta,
                "passed": alert_delta <= self.training_spec.validation_gates.max_alert_volume_delta,
            },
            "max_score_drift": {
                "threshold": self.training_spec.validation_gates.max_score_drift,
                "value": score_drift,
                "passed": score_drift <= self.training_spec.validation_gates.max_score_drift,
            },
        }

        tuning_summary = {
            "method": hpo_method,
            "hpo_method": hpo_method,
            "hpo_fallback_used": hpo_fallback_used,
            "hpo_fallback_activation_count": 1 if hpo_fallback_used else 0,
            "max_trials": self.training_spec.tuning.max_trials,
            "timeout_seconds": self.training_spec.tuning.timeout_seconds,
            "search_space": search_space,
            "trials": trials,
            "best_params": best_trial["params"],
            "baseline_objective": baseline_obj,
            "best_objective": best_obj,
            "relative_improvement": rel_improvement,
        }
        val_metrics = {
            "alert_threshold": alert_threshold,
            "val_alert_rate": val_alert_rate,
            "score_drift": score_drift,
        }
        return tuning_summary, best_trial["params"], gates, val_metrics

    def _fit_final_model(self, full_processed, selected_features: List[str], best_params: Dict[str, Any]):
        """Execute the fit final model stage of the workflow."""
        from sklearn.ensemble import IsolationForest

        full_pdf = full_processed.select(*selected_features).toPandas().fillna(0.0)
        x_full = full_pdf.to_numpy(dtype=float)
        if x_full.size == 0:
            raise ValueError("Full-window refit dataset is empty")
        return IsolationForest(**best_params, random_state=self.training_spec.random_seed, n_jobs=1).fit(x_full)

    def _build_metrics(
        self,
        full_processed,
        train_processed,
        val_processed,
        all_features: List[str],
        selected_features: List[str],
        tuning_summary: Dict[str, Any],
        gates: Dict[str, Any],
        val_metrics: Dict[str, Any],
    ) -> Dict[str, float]:
        """Execute the build metrics stage of the workflow."""
        return {
            "full_training_row_count": float(full_processed.count()),
            "train_row_count": float(train_processed.count()),
            "validation_row_count": float(val_processed.count()),
            "feature_count_pre_selection": float(len(all_features)),
            "feature_count_post_selection": float(len(selected_features)),
            "best_objective": float(tuning_summary["best_objective"]),
            "relative_improvement": float(tuning_summary["relative_improvement"]),
            "validation_alert_rate": float(val_metrics["val_alert_rate"]),
            "validation_score_drift": float(val_metrics["score_drift"]),
            "gates_passed": float(all(g["passed"] for g in gates.values())),
        }

    def _maybe_deploy(self, model_data_url: str, gates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Execute the maybe deploy stage of the workflow."""
        deployment = self.training_spec.deployment
        if not deployment.deploy_on_success:
            return {"attempted": False, "status": "skipped", "reason": "deploy_on_success=false"}
        if not all(g["passed"] for g in gates.values()):
            return {"attempted": False, "status": "skipped", "reason": "promotion_gates_failed"}
        if not deployment.endpoint_name:
            return {"attempted": False, "status": "failed", "reason": "missing endpoint_name"}

        import boto3

        sm = boto3.client("sagemaker")
        endpoint_name = deployment.endpoint_name
        model_name = f"ndr-if-{self.training_spec.model_version}-{self.runtime_config.run_id}"
        endpoint_cfg = f"{endpoint_name}-cfg-{self.runtime_config.run_id}"

        previous_cfg = None
        endpoint_exists = False
        try:
            desc = sm.describe_endpoint(EndpointName=endpoint_name)
            previous_cfg = desc.get("EndpointConfigName")
            endpoint_exists = True
        except Exception:  # pylint: disable=broad-except
            endpoint_exists = False

        try:
            execution_role_arn = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")
            if not execution_role_arn:
                raise ValueError("SAGEMAKER_EXECUTION_ROLE_ARN is required for deployment")

            image_uri = os.environ.get("IF_INFERENCE_IMAGE_URI", "")
            if not image_uri:
                raise ValueError("IF_INFERENCE_IMAGE_URI is required for deployment")

            self._call_with_retries(
                sm.create_model,
                ModelName=model_name,
                ExecutionRoleArn=execution_role_arn,
                PrimaryContainer={"Image": image_uri, "ModelDataUrl": model_data_url},
            )
            self._call_with_retries(
                sm.create_endpoint_config,
                EndpointConfigName=endpoint_cfg,
                ProductionVariants=[
                    {
                        "VariantName": deployment.variant_name,
                        "ModelName": model_name,
                        "InstanceType": deployment.instance_type,
                        "InitialInstanceCount": deployment.initial_instance_count,
                    }
                ],
            )
            if endpoint_exists:
                self._call_with_retries(sm.update_endpoint, EndpointName=endpoint_name, EndpointConfigName=endpoint_cfg)
            else:
                self._call_with_retries(sm.create_endpoint, EndpointName=endpoint_name, EndpointConfigName=endpoint_cfg)

            return {
                "attempted": True,
                "status": "success",
                "endpoint_name": endpoint_name,
                "endpoint_config": endpoint_cfg,
                "previous_endpoint_config": previous_cfg,
                "rollback_triggered": False,
            }
        except Exception as exc:  # pylint: disable=broad-except
            rollback_triggered = False
            rollback_error = None
            if endpoint_exists and previous_cfg and deployment.rollback_on_alarm:
                try:
                    self._call_with_retries(sm.update_endpoint, EndpointName=endpoint_name, EndpointConfigName=previous_cfg)
                    rollback_triggered = True
                except Exception as rollback_exc:  # pylint: disable=broad-except
                    rollback_error = str(rollback_exc)
            return {
                "attempted": True,
                "status": "failed",
                "endpoint_name": endpoint_name,
                "endpoint_config": endpoint_cfg,
                "previous_endpoint_config": previous_cfg,
                "error": str(exc),
                "rollback_triggered": rollback_triggered,
                "rollback_error": rollback_error,
            }

    def _call_with_retries(self, fn, **kwargs):
        """Execute the call with retries stage of the workflow."""
        retries = max(self.training_spec.reliability.max_retries, 0)
        backoff = max(self.training_spec.reliability.backoff_seconds, 0.0)
        last_exc = None
        for attempt in range(retries + 1):
            try:
                return fn(**kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                if attempt >= retries:
                    break
                time.sleep(backoff * (2 ** attempt))
        raise last_exc

    def _evaluate_option4_guardrail(self) -> Dict[str, Any]:
        """Execute the evaluate option4 guardrail stage of the workflow."""
        cfg = self.training_spec.cost_guardrail
        option1_monthly = cfg.option1_hourly_rate_usd * 24 * 30
        option4_hours = (cfg.option4_batch_minutes / 60.0) * cfg.option4_runs_per_month
        option4_monthly = option4_hours * cfg.option1_hourly_rate_usd
        review_required = cfg.enabled and option1_monthly > cfg.option1_monthly_budget_usd
        return {
            "enabled": cfg.enabled,
            "option1_estimated_monthly_usd": option1_monthly,
            "option4_estimated_monthly_usd": option4_monthly,
            "budget_threshold_usd": cfg.option1_monthly_budget_usd,
            "option4_review_required": review_required,
            "message": (
                "Review Option-4 batch inference trade-offs before switching deployment strategy"
                if review_required
                else "Option-1 estimated cost is within configured threshold"
            ),
        }

    def _persist_artifacts(
        self,
        scaler_params: Dict[str, Dict[str, float]],
        outlier_params: Dict[str, Dict[str, float]],
        feature_mask: List[str],
        metrics: Dict[str, float],
        tuning_summary: Dict[str, Any],
        model,
        train_start: datetime,
        train_end: datetime,
    ) -> Dict[str, str]:
        """Execute the persist artifacts stage of the workflow."""
        import boto3

        bucket, key_prefix = _split_s3_uri(self.training_spec.output.artifacts_s3_prefix)
        client = boto3.client("s3")

        run_prefix = self._artifact_run_prefix(key_prefix, train_start, train_end)
        model_key = f"{run_prefix}/model/model.joblib"
        model_tar_key = f"{run_prefix}/model/model.tar.gz"

        model_hash = self._put_model_artifacts(client, bucket, model, model_key, model_tar_key)
        _put_json(client, bucket, f"{run_prefix}/preprocessing/scaler_params.json", scaler_params)
        _put_json(client, bucket, f"{run_prefix}/preprocessing/outlier_params.json", outlier_params)
        _put_json(client, bucket, f"{run_prefix}/preprocessing/feature_mask.json", feature_mask)
        _put_json(client, bucket, f"{run_prefix}/metrics/final_metrics.json", metrics)
        _put_json(client, bucket, f"{run_prefix}/metrics/tuning_metrics.json", tuning_summary)

        return {
            "bucket": bucket,
            "run_prefix": run_prefix,
            "model_key": model_key,
            "model_tar_key": model_tar_key,
            "model_hash": model_hash,
            "model_tar_s3_uri": f"s3://{bucket}/{model_tar_key}",
        }

    def _promote_latest_model_pointer(self, artifact_ctx: Dict[str, str]) -> str:
        """Execute the promote latest model pointer stage of the workflow."""
        import boto3

        bucket = artifact_ctx["bucket"]
        run_prefix = artifact_ctx["run_prefix"]
        key_prefix = run_prefix.split("/model_version=")[0]
        latest_model_tar_key = (
            f"{key_prefix}/model_version={self.training_spec.model_version}/latest/model/model.tar.gz"
        )
        client = boto3.client("s3")
        client.copy_object(
            Bucket=bucket,
            Key=latest_model_tar_key,
            CopySource={"Bucket": bucket, "Key": artifact_ctx["model_tar_key"]},
        )
        return f"s3://{bucket}/{latest_model_tar_key}"

    def _write_final_report_and_success(
        self,
        train_start: datetime,
        train_end: datetime,
        preflight: Dict[str, Any],
        metadata_columns: List[str],
        selected_features: List[str],
        scaler_params: Dict[str, Dict[str, float]],
        outlier_params: Dict[str, Dict[str, float]],
        tuning_summary: Dict[str, Any],
        metrics: Dict[str, float],
        gates: Dict[str, Any],
        deployment_status: Dict[str, Any],
        selection_meta: Dict[str, Any],
        artifact_ctx: Dict[str, str],
        latest_model_path: str | None,
        history_planner: Dict[str, Any] | None,
        evaluation_manifests: List[Dict[str, Any]],
    ) -> str:
        """Execute the write final report and success stage of the workflow."""
        import boto3
        import sklearn

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        client = boto3.client("s3")

        report = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "ml_project_name": self.runtime_config.ml_project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
                "random_seed": self.training_spec.random_seed,
            },
            "input_datasets": {
                "fg_a_prefix": self.training_spec.feature_inputs["fg_a"].s3_prefix,
                "fg_c_prefix": self.training_spec.feature_inputs["fg_c"].s3_prefix,
                "preflight": preflight,
            },
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
                "lookback_months": self.training_spec.window.lookback_months,
                "gap_months": self.training_spec.window.gap_months,
            },
            "history_planner": history_planner,
            "evaluation": {
                "windows": self._resolve_evaluation_windows(),
                "manifests": evaluation_manifests,
            },
            "schema_and_join": {
                "fg_a_manifest": "build_fg_a_manifest",
                "fg_c_manifest": "build_fg_c_manifest",
                "join_keys": self.training_spec.join_keys,
                "metadata_columns": metadata_columns,
                "selected_feature_columns": selected_features,
            },
            "preprocessing": {
                "eps": self.training_spec.preprocessing.eps,
                "z_max": self.training_spec.preprocessing.z_max,
                "imputation_strategy": self.training_spec.preprocessing.imputation_strategy,
                "imputation_constant": self.training_spec.preprocessing.imputation_constant,
                "scaler_params": scaler_params,
                "outlier_params": outlier_params,
            },
            "feature_selection": {
                **asdict(self.training_spec.feature_selection),
                "selection_meta": selection_meta,
            },
            "tuning": tuning_summary,
            "final_model": {
                "model_path": f"s3://{artifact_ctx['bucket']}/{artifact_ctx['model_key']}",
                "model_image_copy_path": f"s3://{artifact_ctx['bucket']}/{artifact_ctx['model_tar_key']}",
                "latest_model_image_path": latest_model_path,
                "artifact_hash": artifact_ctx["model_hash"],
            },
            "deployment": {**asdict(self.training_spec.deployment), "status": deployment_status},
            "cost_guardrail": self._evaluate_option4_guardrail(),
            "validation_gates": gates,
            "metrics": metrics,
            "deterministic_context": {
                "python_version": os.sys.version,
                "sklearn_version": sklearn.__version__,
                "output_prefix": f"s3://{artifact_ctx['bucket']}/{artifact_ctx['run_prefix']}",
                "runtime_env": {
                    "dpp_config_table_name": self.runtime_config.dpp_config_table_name,
                    "mlp_config_table_name": self.runtime_config.mlp_config_table_name,
                    "batch_index_table_name": self.runtime_config.batch_index_table_name,
                },
                "resolved_job_spec_payload": self.resolved_spec_payload,
            },
        }

        report_key = f"{self._report_run_prefix(report_key_prefix)}/final_training_report.json"
        _put_json(client, report_bucket, report_key, report)

        self._write_inference_preprocessing_back(selected_features, scaler_params, outlier_params)
        client.put_object(Bucket=artifact_ctx["bucket"], Key=f"{artifact_ctx['run_prefix']}/SUCCESS", Body=b"ok")
        return f"s3://{report_bucket}/{report_key}"

    def _write_preflight_failure_artifact(
        self,
        train_start: datetime,
        train_end: datetime,
        reason: str,
        context: Dict[str, Any],
    ) -> None:
        """Execute the write preflight failure artifact stage of the workflow."""
        import boto3

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        key = f"{self._report_run_prefix(report_key_prefix)}/preflight_failure_context.json"
        payload = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "ml_project_name": self.runtime_config.ml_project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
            },
            "failure": {
                "stage": "preflight",
                "reason": reason,
                "context": context,
            },
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
            },
            "resolved_job_spec_payload": self.resolved_spec_payload,
        }
        try:
            client = boto3.client("s3")
            _put_json(client, report_bucket, key, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to write preflight failure artifact: %s", exc)

    def _write_failure_report(self, stage: str, error: str, train_start: datetime, train_end: datetime) -> None:
        """Execute the write failure report stage of the workflow."""
        import boto3

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        key = f"{self._report_run_prefix(report_key_prefix)}/failure_report.json"
        payload = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "ml_project_name": self.runtime_config.ml_project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
            },
            "failure": {
                "stage": stage,
                "error": error,
            },
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
            },
            "resolved_job_spec_payload": self.resolved_spec_payload,
        }
        try:
            client = boto3.client("s3")
            _put_json(client, report_bucket, key, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to write failure report: %s", exc)

    def _write_failure_experiment_artifact(
        self,
        stage: str,
        error: str,
        train_start: datetime,
        train_end: datetime,
    ) -> None:
        """Execute the write failure experiment artifact stage of the workflow."""
        import boto3

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        context_key = f"{self._report_run_prefix(report_key_prefix)}/failure_experiment_context.json"
        payload = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "ml_project_name": self.runtime_config.ml_project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
            },
            "failure": {"stage": stage, "error": error},
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
            },
            "resolved_job_spec_payload": self.resolved_spec_payload,
        }

        try:
            s3 = boto3.client("s3")
            _put_json(s3, report_bucket, context_key, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to persist failure experiment context: %s", exc)
            return

        if not self.training_spec.experiments.enabled:
            return

        try:
            sm = boto3.client("sagemaker")
            experiment_name = self.training_spec.experiments.experiment_name or f"{self.runtime_config.project_name}-if-training"
            trial_name = (
                f"{self.training_spec.experiments.trial_prefix}-{self.runtime_config.feature_spec_version}-{self.runtime_config.run_id}"
            )
            component_name = f"{trial_name}-failure"
            _safe_create_experiment(sm, experiment_name)
            _safe_create_trial(sm, trial_name, experiment_name)
            _safe_create_trial_component(sm, component_name)
            sm.associate_trial_component(TrialComponentName=component_name, TrialName=trial_name)
            sm.update_trial_component(
                TrialComponentName=component_name,
                Parameters={
                    "failure_stage": {"StringValue": stage},
                    "failure_error": {"StringValue": error[:1024]},
                    "failure_context_s3_uri": {
                        "StringValue": f"s3://{report_bucket}/{context_key}"
                    },
                },
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to update failure experiment artifact: %s", exc)

    def _log_sagemaker_experiments(
        self,
        metrics: Dict[str, float],
        tuning_summary: Dict[str, Any],
        deployment_status: Dict[str, Any],
        preflight: Dict[str, Any],
        report_s3_uri: str,
        selected_feature_count: int,
        all_feature_count: int,
        history_planner: Dict[str, Any] | None,
        evaluation_windows: List[Dict[str, Any]],
    ) -> None:
        """Execute the log sagemaker experiments stage of the workflow."""
        if not self.training_spec.experiments.enabled:
            return
        try:
            import boto3

            sm = boto3.client("sagemaker")
            experiment_name = self.training_spec.experiments.experiment_name or f"{self.runtime_config.project_name}-if-training"
            trial_name = (
                f"{self.training_spec.experiments.trial_prefix}-{self.runtime_config.feature_spec_version}-{self.runtime_config.run_id}"
            )
            preprocessing_component = f"{trial_name}-preprocessing"
            tuning_component = f"{trial_name}-tuning"
            final_component = f"{trial_name}-final"

            _safe_create_experiment(sm, experiment_name)
            _safe_create_trial(sm, trial_name, experiment_name)
            for component_name in (preprocessing_component, tuning_component, final_component):
                _safe_create_trial_component(sm, component_name)
                sm.associate_trial_component(TrialComponentName=component_name, TrialName=trial_name)

            planner_component = f"{trial_name}-history-planner"
            _safe_create_trial_component(sm, planner_component)
            sm.associate_trial_component(TrialComponentName=planner_component, TrialName=trial_name)
            sm.update_trial_component(
                TrialComponentName=planner_component,
                Parameters={
                    "history_planner_artifact_present": {"StringValue": str(bool(history_planner))},
                    "history_required_start": {"StringValue": str(((history_planner or {}).get('computed', {}).get('w_required', {}) or {}).get('start', ''))},
                },
            )

            for window in evaluation_windows:
                component_name = f"{trial_name}-eval-{window['window_id']}"
                _safe_create_trial_component(sm, component_name)
                sm.associate_trial_component(TrialComponentName=component_name, TrialName=trial_name)
                sm.update_trial_component(
                    TrialComponentName=component_name,
                    Parameters={
                        "evaluation_window_start": {"StringValue": window["start_ts"]},
                        "evaluation_window_end": {"StringValue": window["end_ts"]},
                    },
                )

            sm.update_trial_component(
                TrialComponentName=preprocessing_component,
                Parameters={
                    "feature_count_before_selection": {"StringValue": str(all_feature_count)},
                    "feature_count_after_selection": {"StringValue": str(selected_feature_count)},
                    "preflight_join_coverage_ratio": {"StringValue": f"{float(preflight.get('join_coverage_ratio', 0.0)):.6f}"},
                    "preflight_partition_coverage": {"StringValue": json.dumps(preflight.get("partition_coverage", {}), sort_keys=True)[:1024]},
                },
            )

            tuning_metrics = [
                {
                    "MetricName": "best_objective",
                    "Value": float(tuning_summary.get("best_objective", 0.0)),
                    "Timestamp": datetime.now(timezone.utc),
                },
                {
                    "MetricName": "baseline_objective",
                    "Value": float(tuning_summary.get("baseline_objective", 0.0)),
                    "Timestamp": datetime.now(timezone.utc),
                },
                {
                    "MetricName": "objective_improvement",
                    "Value": float(tuning_summary.get("objective_improvement", 0.0)),
                    "Timestamp": datetime.now(timezone.utc),
                },
            ]
            sm.batch_put_metrics(TrialComponentName=tuning_component, MetricData=tuning_metrics)

            final_metrics = [
                {"MetricName": k, "Value": float(v), "Timestamp": datetime.now(timezone.utc)}
                for k, v in metrics.items()
            ] + [
                {
                    "MetricName": "deployment_attempted",
                    "Value": 1.0 if deployment_status.get("attempted") else 0.0,
                    "Timestamp": datetime.now(timezone.utc),
                },
            ]
            sm.batch_put_metrics(TrialComponentName=final_component, MetricData=final_metrics)
            sm.update_trial_component(
                TrialComponentName=final_component,
                Parameters={
                    "final_report_s3_uri": {"StringValue": report_s3_uri},
                    "deployment_status": {"StringValue": str(deployment_status.get("status", "unknown"))},
                },
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to log SageMaker Experiments metrics: %s", exc)

    def _write_inference_preprocessing_back(
        self,
        feature_mask: List[str],
        scaler_params: Dict[str, Dict[str, float]],
        outlier_params: Dict[str, Dict[str, float]],
    ) -> None:
        """Execute the write inference preprocessing back stage of the workflow."""
        import boto3

        table_name = (self.runtime_config.dpp_config_table_name or "").strip()
        if not table_name:
            raise ValueError("Missing required runtime parameter: dpp_config_table_name")

        ddb = boto3.resource("dynamodb")
        table = ddb.Table(table_name)
        job_name_version = f"inference_predictions#{self.runtime_config.feature_spec_version}"
        item = table.get_item(
            Key={
                "project_name": self.runtime_config.project_name,
                "job_name_version": job_name_version,
            }
        ).get("Item")
        if not item:
            raise KeyError(f"Inference spec record not found: {job_name_version}")

        spec = item.get("spec", {})
        payload = spec.get("payload", {})
        payload["feature_columns"] = feature_mask
        payload["scaler_params"] = scaler_params
        payload["outlier_params"] = outlier_params
        payload["model_version"] = self.training_spec.model_version
        spec["payload"] = payload

        table.update_item(
            Key={
                "project_name": self.runtime_config.project_name,
                "job_name_version": job_name_version,
            },
            UpdateExpression="SET #spec = :spec",
            ExpressionAttributeNames={"#spec": "spec"},
            ExpressionAttributeValues={":spec": spec},
        )

    def _put_model_artifacts(self, client, bucket: str, model, model_key: str, model_tar_key: str) -> str:
        """Execute the put model artifacts stage of the workflow."""
        import joblib

        model_buffer = io.BytesIO()
        joblib.dump(model, model_buffer)
        model_bytes = model_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=model_key, Body=model_bytes)

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            file_data = io.BytesIO(model_bytes)
            info = tarfile.TarInfo(name="model.joblib")
            info.size = len(model_bytes)
            tar.addfile(info, file_data)
        client.put_object(Bucket=bucket, Key=model_tar_key, Body=tar_buffer.getvalue())

        return hashlib.sha256(model_bytes).hexdigest()


def _safe_create_experiment(client, experiment_name: str) -> None:
    """Execute the safe create experiment stage of the workflow."""
    try:
        client.create_experiment(ExperimentName=experiment_name)
    except Exception as exc:  # pylint: disable=broad-except
        if "already exists" not in str(exc).lower():
            raise


def _safe_create_trial(client, trial_name: str, experiment_name: str) -> None:
    """Execute the safe create trial stage of the workflow."""
    try:
        client.create_trial(TrialName=trial_name, ExperimentName=experiment_name)
    except Exception as exc:  # pylint: disable=broad-except
        if "already exists" not in str(exc).lower():
            raise


def _safe_create_trial_component(client, component_name: str) -> None:
    """Execute the safe create trial component stage of the workflow."""
    try:
        client.create_trial_component(TrialComponentName=component_name)
    except Exception as exc:  # pylint: disable=broad-except
        if "already exists" not in str(exc).lower():
            raise


def _split_s3_uri(uri: str) -> Tuple[str, str]:
    """Execute the split s3 uri stage of the workflow."""
    cleaned = uri.replace("s3://", "", 1)
    bucket, _, key = cleaned.partition("/")
    return bucket, key


def _put_json(client, bucket: str, key: str, payload) -> None:
    """Execute the put json stage of the workflow."""
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def run_if_training_from_runtime_config(runtime_config: IFTrainingRuntimeConfig) -> None:
    """Execute the run if training from runtime config stage of the workflow."""
    from ndr.config.job_spec_loader import load_job_spec
    from pyspark.sql import SparkSession

    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="if_training",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    runtime_defaults = (job_spec.get("runtime_defaults") or {}) if isinstance(job_spec, dict) else {}
    runtime_config = replace(
        runtime_config,
        eval_start_ts=runtime_config.eval_start_ts or runtime_defaults.get("EvalStartTs"),
        eval_end_ts=runtime_config.eval_end_ts or runtime_defaults.get("EvalEndTs"),
    )
    training_spec = parse_if_training_spec(job_spec)
    spark = SparkSession.builder.getOrCreate()
    IFTrainingJob(spark, runtime_config, training_spec, resolved_spec_payload=job_spec).run()
