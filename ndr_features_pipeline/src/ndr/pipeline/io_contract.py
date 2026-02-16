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

    @property
    def script_s3_uri(self) -> str:
        return f"{self.code_prefix_s3.rstrip('/')}/{self.entry_script}"


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
    if not isinstance(code_prefix_s3, str) or not code_prefix_s3.strip():
        raise ValueError(f"scripts.steps.{step_name}.code_prefix_s3 must be a non-empty string")
    if not isinstance(entry_script, str) or not entry_script.strip():
        raise ValueError(f"scripts.steps.{step_name}.entry_script must be a non-empty string")

    return StepScriptContract(
        code_prefix_s3=code_prefix_s3,
        entry_script=entry_script,
    )


def resolve_step_code_uri(
    *,
    project_name: str,
    feature_spec_version: str,
    pipeline_job_name: str,
    step_name: str,
    table_name: str | None = None,
) -> str:
    """Load pipeline spec from DynamoDB and resolve a step's script S3 URI."""
    pipeline_spec = load_job_spec(
        project_name=project_name,
        job_name=pipeline_job_name,
        feature_spec_version=feature_spec_version,
        table_name=table_name,
    )
    contract = resolve_step_script_contract(pipeline_spec, step_name=step_name)
    return contract.script_s3_uri
