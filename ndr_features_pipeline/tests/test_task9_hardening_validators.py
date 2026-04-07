"""Task 9 hardening validators for fallback elimination and targeted recovery."""

import pytest

from ndr.orchestration.contract_validators import (
    validate_no_business_fallback_markers,
    validate_targeted_recovery_manifest,
)


def test_task9_rejects_placeholder_or_env_fallback_markers():
    with pytest.raises(ValueError, match="fallback/placeholder business markers"):
        validate_no_business_fallback_markers(
            context="task9.rollout",
            values={
                "resolved_target": "${PipelineNameInference}",
                "resolution_source": "env_fallback",
            },
        )


def test_task9_accepts_concrete_ddb_resolved_values():
    validate_no_business_fallback_markers(
        context="task9.rollout",
        values={
            "resolved_target": "ndr-inference-pipeline-prod",
            "resolution_source": "ddb_override",
            "artifact_sha256": "a" * 64,
        },
    )


def test_task9_targeted_recovery_manifest_rejects_non_selective_family():
    with pytest.raises(ValueError, match="unsupported family"):
        validate_targeted_recovery_manifest(
            requested_families=["fg_c"],
            manifest_entries=[
                {
                    "artifact_family": "fg_b",
                    "ranges": [{"start_ts": "2024-01-01T00:00:00Z", "end_ts": "2024-01-01T01:00:00Z"}],
                }
            ],
        )


def test_task9_targeted_recovery_manifest_rejects_missing_ranges():
    with pytest.raises(ValueError, match="non-empty ranges"):
        validate_targeted_recovery_manifest(
            requested_families=["fg_c"],
            manifest_entries=[{"artifact_family": "fg_c", "ranges": []}],
        )


def test_task9_targeted_recovery_manifest_accepts_requested_families_only():
    validate_targeted_recovery_manifest(
        requested_families=["fg_a", "fg_c"],
        manifest_entries=[
            {
                "artifact_family": "fg_a",
                "ranges": [{"start_ts": "2024-01-01T00:00:00Z", "end_ts": "2024-01-01T01:00:00Z"}],
            },
            {
                "artifact_family": "fg_c",
                "ranges": [{"start_ts": "2024-01-01T02:00:00Z", "end_ts": "2024-01-01T03:00:00Z"}],
            },
        ],
    )
