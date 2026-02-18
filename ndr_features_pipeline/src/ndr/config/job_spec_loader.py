"""NDR job spec loader module."""
import os

from typing import Any, Dict

import boto3

from .job_spec_models import (
    JobSpec,
    InputSpec,
    DQSpec,
    EnrichmentSpec,
    RoleMappingSpec,
    OperatorSpec,
    OutputSpec,
    PairCountsJobSpec,
    PairCountsTrafficInputSpec,
    PairCountsFilterSpec,
    PairCountsOutputSpec,
)

LEGACY_DDB_TABLE_ENV_VAR = "JOB_SPEC_DDB_TABLE_NAME"
DDB_TABLE_ENV_VAR = "ML_PROJECTS_PARAMETERS_TABLE_NAME"
JOB_SPEC_SORT_KEY_DELIMITER = "#"


class JobSpecLoader:
    """Loads JobSpec records from a DynamoDB table.

    The table is expected to have a primary key on (project_name, job_name)
    and an attribute 'spec' that contains a JSON-like dictionary compatible
    with the JobSpec dataclasses.
    """

    def __init__(self, table_name: str | None = None):
        """Initialize the instance with required clients and runtime configuration."""
        self._ddb = boto3.resource("dynamodb")
        self._table_name = (
            table_name
            or os.environ.get(DDB_TABLE_ENV_VAR)
            or os.environ.get(LEGACY_DDB_TABLE_ENV_VAR)
        )
        if not self._table_name:
            raise ValueError(
                "DynamoDB table name for JobSpec must be provided or set in "
                f"{DDB_TABLE_ENV_VAR} (or legacy {LEGACY_DDB_TABLE_ENV_VAR})"
            )
        self._table = self._ddb.Table(self._table_name)

    def load(
        self,
        project_name: str,
        job_name: str,
        feature_spec_version: str | None = None,
    ) -> JobSpec:
        """Load a JobSpec from DynamoDB."""
        key = {"project_name": project_name}
        if feature_spec_version:
            sort_key = f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}{feature_spec_version}"
            key["job_name"] = sort_key
        else:
            key["job_name"] = job_name
        response = self._table.get_item(Key=key)
        if "Item" not in response:
            raise KeyError(
                "No JobSpec found for "
                f"project={project_name}, job={job_name}, "
                f"feature_spec_version={feature_spec_version}"
            )
        spec_payload: Dict[str, Any] = response["Item"]["spec"]
        return self._from_dict(spec_payload)

    def _from_dict(self, payload: Dict[str, Any]) -> JobSpec:
        """Construct a JobSpec dataclass from a plain dictionary."""
        input_spec = InputSpec(**payload["input"])
        dq_spec = DQSpec(**payload["dq"])
        enrichment_spec = EnrichmentSpec(**payload.get("enrichment", {}))
        roles = [RoleMappingSpec(**r) for r in payload["roles"]]
        operators = [OperatorSpec(**op) for op in payload["operators"]]
        output_spec = OutputSpec(**payload["output"])
        pair_context_payload = payload.get("pair_context_output")
        pair_context_output = OutputSpec(**pair_context_payload) if pair_context_payload else None
        return JobSpec(
            project_name=payload["project_name"],
            job_name=payload["job_name"],
            feature_spec_version=payload["feature_spec_version"],
            input=input_spec,
            dq=dq_spec,
            enrichment=enrichment_spec,
            roles=roles,
            operators=operators,
            output=output_spec,
            pair_context_output=pair_context_output,
        )


def load_job_spec(
    project_name: str,
    job_name: str,
    feature_spec_version: str,
    table_name: str | None = None,
) -> Dict[str, Any]:
    """Load a JobSpec payload as a plain dictionary."""
    loader = JobSpecLoader(table_name=table_name)
    response = loader._table.get_item(
        Key={
            "project_name": project_name,
            "job_name": f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}{feature_spec_version}",
        }
    )
    if "Item" not in response:
        raise KeyError(
            "No JobSpec found for "
            f"project={project_name}, job={job_name}, "
            f"feature_spec_version={feature_spec_version}"
        )
    return response["Item"]["spec"]


def load_pair_counts_job_spec(
    project_name: str,
    feature_spec_version: str,
    table_name: str | None = None,
) -> PairCountsJobSpec:
    """Load a strongly typed Pair-Counts JobSpec payload from DynamoDB."""
    payload = load_job_spec(
        project_name=project_name,
        job_name="pair_counts_builder",
        feature_spec_version=feature_spec_version,
        table_name=table_name,
    )

    traffic_payload: Dict[str, Any] = payload.get("traffic_input", {})
    output_payload: Dict[str, Any] = payload.get("pair_counts_output", {})
    filters_payload: Dict[str, Any] = payload.get("filters", {})

    traffic_input = PairCountsTrafficInputSpec(
        s3_prefix=traffic_payload["s3_prefix"],
        layout=traffic_payload.get("layout", "batch_folder"),
        field_mapping=dict(traffic_payload.get("field_mapping", {})),
    )
    if not traffic_input.field_mapping:
        raise ValueError(
            "pair_counts_builder.traffic_input.field_mapping is required and must map "
            "source_ip/destination_ip/destination_port/event_start/event_end to source columns."
        )

    required_mapping_keys = {
        "source_ip",
        "destination_ip",
        "destination_port",
        "event_start",
        "event_end",
    }
    missing_mapping_keys = sorted(required_mapping_keys - set(traffic_input.field_mapping.keys()))
    if missing_mapping_keys:
        raise ValueError(
            "pair_counts_builder.traffic_input.field_mapping missing required keys: "
            f"{missing_mapping_keys}"
        )

    return PairCountsJobSpec(
        traffic_input=traffic_input,
        pair_counts_output=PairCountsOutputSpec(s3_prefix=output_payload["s3_prefix"]),
        filters=PairCountsFilterSpec(**filters_payload),
        segment_mapping=dict(payload.get("segment_mapping", {})),
    )
