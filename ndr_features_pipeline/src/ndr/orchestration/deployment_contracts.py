from __future__ import annotations

"""Deployment lifecycle contracts for code artifact promotion and rollback."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping

READY_STATUS = "READY"
IN_PROGRESS_STATUS = "IN_PROGRESS"
FAILED_STATUS = "FAILED"
RECONCILIATION_REQUIRED_CHECKPOINT = "break_glass_reconciliation_required"

VALID_DEPLOYMENT_MODES = {"shadow", "authoritative"}


@dataclass(frozen=True)
class DeploymentRequest:
    project_name: str
    feature_spec_version: str
    deployment_mode: str
    request_id: str
    artifact_build_id: str
    force_reconciliation: bool = False



def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")



def normalize_deployment_request(payload: Mapping[str, Any]) -> DeploymentRequest:
    project_name = str(payload.get("project_name") or "").strip()
    feature_spec_version = str(payload.get("feature_spec_version") or "").strip()
    deployment_mode = str(payload.get("deployment_mode") or "shadow").strip().lower()
    request_id = str(payload.get("request_id") or "").strip()
    artifact_build_id = str(payload.get("artifact_build_id") or "").strip()
    if not project_name:
        raise ValueError("TASK9_INVALID_DEPLOYMENT_REQUEST: project_name is required")
    if not feature_spec_version:
        raise ValueError("TASK9_INVALID_DEPLOYMENT_REQUEST: feature_spec_version is required")
    if deployment_mode not in VALID_DEPLOYMENT_MODES:
        raise ValueError("TASK9_INVALID_DEPLOYMENT_MODE: deployment_mode must be shadow|authoritative")
    if not request_id:
        raise ValueError("TASK9_INVALID_DEPLOYMENT_REQUEST: request_id is required")
    if not artifact_build_id:
        deterministic = sha256(f"{project_name}|{feature_spec_version}|{request_id}".encode("utf-8")).hexdigest()
        artifact_build_id = f"auto-{deterministic[:16]}"
    return DeploymentRequest(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        deployment_mode=deployment_mode,
        request_id=request_id,
        artifact_build_id=artifact_build_id,
        force_reconciliation=bool(payload.get("force_reconciliation")),
    )



def enforce_break_glass_reconciliation(
    *,
    pipeline_spec: Mapping[str, Any],
    request: DeploymentRequest,
) -> None:
    marker = str(pipeline_spec.get("deployment_manual_marker") or "").strip()
    checkpoint = str(pipeline_spec.get("deployment_checkpoint") or "").strip()
    if marker and checkpoint == RECONCILIATION_REQUIRED_CHECKPOINT and not request.force_reconciliation:
        raise ValueError(
            "TASK9_BREAK_GLASS_RECONCILIATION_REQUIRED: manual deployment marker detected; "
            "run with force_reconciliation=true before next authoritative promotion"
        )



def should_mutate_pointers(request: DeploymentRequest) -> bool:
    return request.deployment_mode == "authoritative"



def build_promotion_spec(
    *,
    pipeline_spec: Mapping[str, Any],
    request: DeploymentRequest,
    promoted_steps: Mapping[str, Mapping[str, str]],
    updated_at: str | None = None,
) -> dict[str, Any]:
    """Build promoted spec with previous pointer capture and optimistic revision bump."""
    if request.deployment_mode != "authoritative":
        raise ValueError("TASK9_PROMOTION_MODE_ERROR: pointer promotion is only allowed in authoritative mode")

    updated_at = updated_at or _utc_now()
    next_spec = deepcopy(dict(pipeline_spec))
    scripts = next_spec.setdefault("scripts", {})
    steps = scripts.setdefault("steps", {})

    for step_name, next_meta in promoted_steps.items():
        current = dict(steps.get(step_name) or {})
        if not current:
            raise ValueError(f"TASK9_UNKNOWN_STEP: cannot promote unknown step {step_name}")
        for field in ("code_artifact_s3_uri", "artifact_build_id", "artifact_sha256", "artifact_format"):
            current[f"previous_{field}"] = current.get(field, "")
            if field not in next_meta or not str(next_meta[field]).strip():
                raise ValueError(f"TASK9_INVALID_PROMOTION_PAYLOAD: missing {field} for {step_name}")
            current[field] = str(next_meta[field]).strip()
        steps[step_name] = current

    revision = int(next_spec.get("deployment_revision") or 0)
    next_spec["deployment_revision"] = revision + 1
    next_spec["deployment_status"] = IN_PROGRESS_STATUS
    next_spec["deployment_checkpoint"] = "artifact_promoted_pending_smoke"
    next_spec["deployment_last_build_id"] = request.artifact_build_id
    next_spec["deployment_last_error"] = ""
    next_spec["deployment_updated_at"] = updated_at
    next_spec["deployment_last_request_id"] = request.request_id
    return next_spec



def build_ready_commit_spec(*, pipeline_spec: Mapping[str, Any], request: DeploymentRequest, updated_at: str | None = None) -> dict[str, Any]:
    updated_at = updated_at or _utc_now()
    committed = deepcopy(dict(pipeline_spec))
    committed["deployment_status"] = READY_STATUS
    committed["deployment_checkpoint"] = "steady_state_ready"
    committed["deployment_last_build_id"] = request.artifact_build_id
    committed["deployment_last_error"] = ""
    committed["deployment_updated_at"] = updated_at
    committed["deployment_manual_marker"] = ""
    return committed



def build_rollback_spec(
    *,
    pipeline_spec: Mapping[str, Any],
    request: DeploymentRequest,
    rollback_reason: str,
    updated_at: str | None = None,
) -> dict[str, Any]:
    updated_at = updated_at or _utc_now()
    rolled_back = deepcopy(dict(pipeline_spec))
    scripts = rolled_back.get("scripts", {})
    steps = scripts.get("steps", {}) if isinstance(scripts, Mapping) else {}
    for step_name, step_spec in steps.items():
        if not isinstance(step_spec, Mapping):
            continue
        for field in ("code_artifact_s3_uri", "artifact_build_id", "artifact_sha256", "artifact_format"):
            previous = step_spec.get(f"previous_{field}")
            if previous:
                step_spec[field] = previous

    rolled_back["deployment_status"] = FAILED_STATUS
    rolled_back["deployment_checkpoint"] = "rollback_completed"
    rolled_back["deployment_last_error"] = rollback_reason
    rolled_back["deployment_updated_at"] = updated_at
    rolled_back["deployment_last_request_id"] = request.request_id
    return rolled_back



def assert_atomic_promotion_revision(*, expected_revision: int, current_spec: Mapping[str, Any]) -> None:
    current_revision = int(current_spec.get("deployment_revision") or 0)
    if current_revision != expected_revision:
        raise ValueError(
            f"TASK9_PROMOTION_CONTENTION: expected deployment_revision={expected_revision}, "
            f"observed={current_revision}"
        )
