"""CI guard: fail on undeclared interface drift from frozen flow_contract_matrix_v3."""

from __future__ import annotations

from ndr.contracts_v2 import assert_contract_surface, load_contract_matrix


def main() -> int:
    matrix = load_contract_matrix()

    assert_contract_surface(
        matrix=matrix,
        edge_id="monthly_readiness_artifact",
        layer="artifact",
        observed_fields={
            "contract_version",
            "project_name",
            "feature_spec_version",
            "reference_month",
            "ready",
            "required_families",
            "missing_ranges",
            "decision_code",
            "as_of_ts",
            "idempotency_key",
        },
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="rt_readiness_artifact",
        layer="artifact",
        observed_fields={
            "contract_version",
            "project_name",
            "feature_spec_version",
            "ml_project_name",
            "mini_batch_id",
            "ready",
            "required_families",
            "missing_ranges",
            "decision_code",
            "as_of_ts",
            "idempotency_key",
        },
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="backfill_request_v2",
        layer="sfn_payload",
        observed_fields={
            "contract_version",
            "consumer",
            "project_name",
            "feature_spec_version",
            "requested_families",
            "missing_ranges",
            "provenance",
            "idempotency_key",
        },
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="step_code_artifact",
        layer="ddb_step_spec",
        observed_fields={
            "code_prefix_s3",
            "code_artifact_s3_uri",
            "entry_script",
            "artifact_build_id",
            "artifact_sha256",
            "artifact_format",
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
