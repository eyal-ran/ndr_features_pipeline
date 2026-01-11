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
)

DDB_TABLE_ENV_VAR = "JOB_SPEC_DDB_TABLE_NAME"


class JobSpecLoader:
    """Loads JobSpec records from a DynamoDB table.

    The table is expected to have a primary key on (project_name, job_name)
    and an attribute 'spec' that contains a JSON-like dictionary compatible
    with the JobSpec dataclasses.
    """

    def __init__(self, table_name: str | None = None):
        self._ddb = boto3.resource("dynamodb")
        self._table_name = table_name or os.environ.get(DDB_TABLE_ENV_VAR)
        if not self._table_name:
            raise ValueError(
                "DynamoDB table name for JobSpec must be provided or set in "
                "JOB_SPEC_DDB_TABLE_NAME"
            )
        self._table = self._ddb.Table(self._table_name)

    def load(self, project_name: str, job_name: str) -> JobSpec:
        """Load a JobSpec from DynamoDB."""
        response = self._table.get_item(
            Key={"project_name": project_name, "job_name": job_name}
        )
        if "Item" not in response:
            raise KeyError(
                f"No JobSpec found for project={project_name}, job={job_name}"
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
        )
