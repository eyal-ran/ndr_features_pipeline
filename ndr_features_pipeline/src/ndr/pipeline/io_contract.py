from __future__ import annotations

"""Helpers for enforcing DynamoDB-driven pipeline IO contracts."""

from dataclasses import dataclass
from typing import Any, Mapping

from ndr.config.job_spec_loader import load_job_spec


@dataclass(frozen=True)
class StepScriptContract:
    """Resolved script contract for a single ProcessingStep."""

    code_prefix_s3: str
    entry_script: str
    code_artifact_s3_uri: str | None = None
    artifact_build_id: str | None = None
    artifact_sha256: str | None = None
    artifact_format: str | None = None

    @property
    def script_s3_uri(self) -> str:
        return self.code_artifact_s3_uri or f"{self.code_prefix_s3.rstrip('/')}/{self.entry_script}"


REQUIRED_CODE_METADATA_FIELDS = (
    "code_artifact_s3_uri",
    "artifact_build_id",
    "artifact_sha256",
    "artifact_format",
)
_FORBIDDEN_IDENTITY_MARKERS = ("<required:", "<placeholder", "${", "env_fallback", "code_default")


def _ensure_mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Expected mapping for '{field_name}', got {type(value).__name__}")
    return value


def resolve_step_script_contract(
    pipeline_spec: Mapping[str, Any],
    step_name: str,
) -> StepScriptContract:
    """Resolve and validate step script configuration from a pipeline spec.

    Expected layout::

        {
          "scripts": {
            "steps": {
              "DeltaBuilderStep": {
                "code_prefix_s3": "s3://.../DeltaBuilderStep/",
                "entry_script": "run_delta_builder.py"
              }
            }
          }
        }
    """
    scripts = _ensure_mapping(pipeline_spec.get("scripts"), field_name="scripts")
    steps = _ensure_mapping(scripts.get("steps"), field_name="scripts.steps")
    step_spec = _ensure_mapping(steps.get(step_name), field_name=f"scripts.steps.{step_name}")

    code_prefix_s3 = step_spec.get("code_prefix_s3")
    entry_script = step_spec.get("entry_script")
    code_artifact_s3_uri = step_spec.get("code_artifact_s3_uri")
    artifact_build_id = step_spec.get("artifact_build_id")
    artifact_sha256 = step_spec.get("artifact_sha256")
    artifact_format = step_spec.get("artifact_format")
    if not isinstance(code_prefix_s3, str) or not code_prefix_s3.strip():
        raise ValueError(f"scripts.steps.{step_name}.code_prefix_s3 must be a non-empty string")
    if not isinstance(entry_script, str) or not entry_script.strip():
        raise ValueError(f"scripts.steps.{step_name}.entry_script must be a non-empty string")

    return StepScriptContract(
        code_prefix_s3=code_prefix_s3,
        entry_script=entry_script,
        code_artifact_s3_uri=code_artifact_s3_uri if isinstance(code_artifact_s3_uri, str) else None,
        artifact_build_id=artifact_build_id if isinstance(artifact_build_id, str) else None,
        artifact_sha256=artifact_sha256 if isinstance(artifact_sha256, str) else None,
        artifact_format=artifact_format if isinstance(artifact_format, str) else None,
    )


def validate_step_code_metadata(
    pipeline_spec: Mapping[str, Any],
    *,
    step_name: str,
    required_fields: tuple[str, ...] = REQUIRED_CODE_METADATA_FIELDS,
) -> Mapping[str, Any]:
    """Validate required packaging metadata for a single pipeline step."""
    scripts = _ensure_mapping(pipeline_spec.get("scripts"), field_name="scripts")
    steps = _ensure_mapping(scripts.get("steps"), field_name="scripts.steps")
    step_spec = _ensure_mapping(steps.get(step_name), field_name=f"scripts.steps.{step_name}")
    code_metadata_raw = step_spec.get("code_metadata")
    if code_metadata_raw is None:
        code_metadata_raw = {
            "code_artifact_s3_uri": step_spec.get("code_artifact_s3_uri"),
            "artifact_build_id": step_spec.get("artifact_build_id"),
            "artifact_sha256": step_spec.get("artifact_sha256"),
            "artifact_format": step_spec.get("artifact_format"),
        }
    if not isinstance(code_metadata_raw, Mapping):
        raise ValueError(
            f"Packaging metadata decision required: scripts.steps.{step_name}.code_metadata is missing"
        )
    code_metadata = code_metadata_raw

    missing_fields = [
        field_name
        for field_name in required_fields
        if not isinstance(code_metadata.get(field_name), str) or not code_metadata.get(field_name, "").strip()
    ]
    if missing_fields:
        missing = ", ".join(missing_fields)
        raise ValueError(
            f"Packaging metadata decision required: scripts.steps.{step_name}.code_metadata missing {missing}"
        )

    return code_metadata


def resolve_step_code_uri(
    *,
    project_name: str,
    feature_spec_version: str,
    pipeline_job_name: str,
    step_name: str,
    table_name: str | None = None,
) -> str:
    """Load pipeline spec from DynamoDB and resolve a step's script S3 URI."""
    for field_name, value in {
        "project_name": project_name,
        "feature_spec_version": feature_spec_version,
        "pipeline_job_name": pipeline_job_name,
    }.items():
        normalized_value = (value or "").strip()
        if not normalized_value:
            raise ValueError(
                f"{field_name} must be a concrete value for step code resolution; received {value!r}"
            )
        lowered_value = normalized_value.lower()
        if any(marker in lowered_value for marker in _FORBIDDEN_IDENTITY_MARKERS):
            raise ValueError(
                f"{field_name} must be a concrete value for step code resolution; received {value!r}"
            )

    pipeline_spec = load_job_spec(
        project_name=project_name,
        job_name=pipeline_job_name,
        feature_spec_version=feature_spec_version,
        table_name=table_name,
    )
    if pipeline_job_name == "pipeline_if_training":
        validate_step_code_metadata(pipeline_spec, step_name=step_name)
    contract = resolve_step_script_contract(pipeline_spec, step_name=step_name)
    return contract.script_s3_uri
