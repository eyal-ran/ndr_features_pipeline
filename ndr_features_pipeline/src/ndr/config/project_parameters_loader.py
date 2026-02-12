"""NDR project parameters loader module."""

from __future__ import annotations


import os
from typing import Any, Dict

import boto3

from .job_spec_loader import DDB_TABLE_ENV_VAR, LEGACY_DDB_TABLE_ENV_VAR, JOB_SPEC_SORT_KEY_DELIMITER

DEFAULT_PROJECT_PARAMETERS_JOB_NAME = "project_parameters"


class ProjectParametersLoader:
    """Loads project-level parameters from the ML projects DynamoDB table."""

    def __init__(self, table_name: str | None = None) -> None:
        """Initialize the instance with required clients and runtime configuration."""
        self._ddb = boto3.resource("dynamodb")
        self._table_name = (
            table_name
            or os.environ.get(DDB_TABLE_ENV_VAR)
            or os.environ.get(LEGACY_DDB_TABLE_ENV_VAR)
        )
        if not self._table_name:
            raise ValueError(
                "DynamoDB table name for project parameters must be provided or set in "
                f"{DDB_TABLE_ENV_VAR} (or legacy {LEGACY_DDB_TABLE_ENV_VAR})"
            )
        self._table = self._ddb.Table(self._table_name)

    def load(
        self,
        project_name: str,
        feature_spec_version: str,
        job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
    ) -> Dict[str, Any]:
        """Load project parameters from DynamoDB for the requested project/version."""
        key = {
            "project_name": project_name,
            "job_name": f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}{feature_spec_version}",
        }
        response = self._table.get_item(Key=key)
        if "Item" not in response:
            raise KeyError(
                "No project parameters found for "
                f"project={project_name}, job={job_name}, "
                f"feature_spec_version={feature_spec_version}"
            )
        item = response["Item"]
        if "spec" in item:
            return item["spec"]
        if "parameters" in item:
            return item["parameters"]
        raise KeyError(
            "Project parameters item missing 'spec' or 'parameters' payload for "
            f"project={project_name}, job={job_name}, "
            f"feature_spec_version={feature_spec_version}"
        )


def load_project_parameters(
    project_name: str,
    feature_spec_version: str,
    table_name: str | None = None,
    job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
) -> Dict[str, Any]:
    """Load project-level parameters from DynamoDB as a dictionary."""
    loader = ProjectParametersLoader(table_name=table_name)
    return loader.load(
        project_name=project_name,
        feature_spec_version=feature_spec_version,
        job_name=job_name,
    )
