"""NDR project parameters loader module."""

from __future__ import annotations


import os
from typing import Any, Dict

from boto3.dynamodb.conditions import Key

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


def resolve_feature_spec_version(
    project_name: str,
    preferred_feature_spec_version: str | None = None,
    table_name: str | None = None,
    job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
) -> str:
    """Resolve feature spec version for a project from project-parameters records."""
    loader = ProjectParametersLoader(table_name=table_name)

    if preferred_feature_spec_version:
        loader.load(
            project_name=project_name,
            feature_spec_version=preferred_feature_spec_version,
            job_name=job_name,
        )
        return preferred_feature_spec_version

    response = loader._table.query(
        KeyConditionExpression=Key("project_name").eq(project_name) & Key("job_name").begins_with(f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}"),
    )
    items = response.get("Items", [])
    if not items:
        raise KeyError(f"No project_parameters records found for project={project_name}")

    def _item_rank(item: Dict[str, Any]) -> tuple[str, str]:
        updated_at = str(item.get("updated_at", ""))
        item_job_name = str(item.get("job_name", ""))
        return (updated_at, item_job_name)

    best = sorted(items, key=_item_rank, reverse=True)[0]
    job_name_value = str(best["job_name"])
    _, feature_spec_version = job_name_value.split(JOB_SPEC_SORT_KEY_DELIMITER, 1)
    return feature_spec_version
