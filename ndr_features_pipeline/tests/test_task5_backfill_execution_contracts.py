import json

import pytest

from ndr.orchestration.backfill_execution_contract import (
    CONTRACT_ERROR_CODE,
    build_execution_request,
    parse_artifact_families,
)
from ndr.scripts.run_backfill_reprocessing_executor import main


def test_family_parser_expands_dependencies_in_deterministic_order_for_mixed_input():
    assert parse_artifact_families("fg_c,fg_b_baseline") == (
        "delta",
        "fg_a",
        "pair_counts",
        "fg_b_baseline",
        "fg_c",
    )


def test_family_parser_supports_alias_for_backward_safe_rollout():
    assert parse_artifact_families("fg_a_15m") == ("delta", "fg_a")


def test_execution_request_fails_fast_with_explicit_error_code_for_contract_violation():
    with pytest.raises(ValueError, match=CONTRACT_ERROR_CODE):
        build_execution_request(
            project_name="ndr-prod",
            feature_spec_version="v1",
            artifact_family="delta",
            range_start_ts_iso="2025-01-01T00:15:00Z",
            range_end_ts_iso="2025-01-01T00:15:00Z",
        )


def test_executor_cli_emits_completion_payload_with_deterministic_idempotency_key(capsys):
    rc = main(
        [
            "--project-name",
            "ndr-prod",
            "--feature-spec-version",
            "v1",
            "--artifact-family",
            "fg_c",
            "--range-start-ts-iso",
            "2025-01-01T00:00:00Z",
            "--range-end-ts-iso",
            "2025-01-01T00:15:00Z",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["status"] == "Succeeded"
    assert payload["artifact_families"] == ["delta", "fg_a", "pair_counts", "fg_b_baseline", "fg_c"]
    assert payload["idempotency_key"].startswith("bkf-")
