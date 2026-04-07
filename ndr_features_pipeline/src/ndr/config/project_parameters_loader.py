"""NDR control-plane config loaders for split DPP/MLP tables."""

from __future__ import annotations

import os
from typing import Any, Dict

from boto3.dynamodb.conditions import Key

import boto3

JOB_SPEC_SORT_KEY_DELIMITER = "#"
DEFAULT_PROJECT_PARAMETERS_JOB_NAME = "project_parameters"

DPP_CONFIG_TABLE_ENV_VAR = "DPP_CONFIG_TABLE_NAME"
DEFAULT_DPP_CONFIG_TABLE_NAME = "dpp_config"

MLP_CONFIG_TABLE_ENV_VAR = "MLP_CONFIG_TABLE_NAME"
DEFAULT_MLP_CONFIG_TABLE_NAME = "mlp_config"

BATCH_INDEX_TABLE_ENV_VAR = "BATCH_INDEX_TABLE_NAME"
DEFAULT_BATCH_INDEX_TABLE_NAME = "batch_index"


class ProjectParametersLoader:
    """Loads project-level parameters from split DPP/MLP config tables."""

    def __init__(
        self,
        dpp_table_name: str | None = None,
        mlp_table_name: str | None = None,
    ) -> None:
        self._ddb = boto3.resource("dynamodb")
        self._dpp_table_name = resolve_dpp_config_table_name(dpp_table_name)
        self._mlp_table_name = resolve_mlp_config_table_name(mlp_table_name)
        self._dpp_table = self._ddb.Table(self._dpp_table_name)
        self._mlp_table = self._ddb.Table(self._mlp_table_name)

    def load(
        self,
        project_name: str,
        feature_spec_version: str,
        job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
    ) -> Dict[str, Any]:
        item = self._load_dpp_item(project_name, feature_spec_version, job_name)
        self._validate_reciprocal_linkage(dpp_item=item)

        if "spec" in item:
            return item["spec"]
        if "parameters" in item:
            return item["parameters"]
        raise KeyError(
            "DPP config item missing 'spec' or 'parameters' payload for "
            f"project={project_name}, job={job_name}, "
            f"feature_spec_version={feature_spec_version}"
        )

    def load_ml_project_names(
        self,
        *,
        project_name: str,
        feature_spec_version: str,
        job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
    ) -> list[str]:
        item = self._load_dpp_item(project_name, feature_spec_version, job_name)
        return self._normalized_ml_project_names(item)

    def load_dpp_s3_roots(self, *, project_name: str) -> Dict[str, Any]:
        return self._load_family_item(self._dpp_table, pk_name="project_name", pk_value=project_name, sk_value="S3_ROOTS")

    def load_mlp_s3_roots(self, *, ml_project_name: str) -> Dict[str, Any]:
        return self._load_family_item(
            self._mlp_table,
            pk_name="ml_project_name",
            pk_value=ml_project_name,
            sk_value="S3_ROOTS",
        )

    def _load_dpp_item(self, project_name: str, feature_spec_version: str, job_name: str) -> Dict[str, Any]:
        response = self._dpp_table.get_item(
            Key={
                "project_name": project_name,
                "job_name_version": f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}{feature_spec_version}",
            }
        )
        if "Item" not in response:
            raise KeyError(
                "No project parameters found for "
                f"project={project_name}, job={job_name}, "
                f"feature_spec_version={feature_spec_version}"
            )
        return response["Item"]

    def _load_family_item(self, table: Any, *, pk_name: str, pk_value: str, sk_value: str) -> Dict[str, Any]:
        response = table.get_item(Key={pk_name: pk_value, "job_name_version": sk_value})
        if "Item" in response:
            return response["Item"]
        response = table.get_item(Key={pk_name: pk_value, "SK": sk_value})
        if "Item" not in response:
            raise KeyError(f"Missing family item pk={pk_value}, sk={sk_value}")
        return response["Item"]

    def _normalized_ml_project_names(self, dpp_item: Dict[str, Any]) -> list[str]:
        names = dpp_item.get("ml_project_names")
        if names is None:
            scalar = str(dpp_item.get("ml_project_name", "")).strip()
            names = [scalar] if scalar else []

        normalized: list[str] = []
        for value in names:
            candidate = str(value).strip()
            if not candidate:
                raise ValueError("ml_project_names cannot contain empty values")
            if candidate not in normalized:
                normalized.append(candidate)
        if not normalized:
            project_name = str(dpp_item.get("project_name", "")).strip()
            raise KeyError(f"DPP config for project={project_name} missing required ml_project_names linkage")
        return normalized

    def _validate_reciprocal_linkage(self, dpp_item: Dict[str, Any]) -> None:
        project_name = str(dpp_item.get("project_name", "")).strip()
        for ml_project_name in self._normalized_ml_project_names(dpp_item):
            mlp_response = self._mlp_table.get_item(
                Key={
                    "ml_project_name": ml_project_name,
                    "job_name_version": dpp_item.get("job_name_version"),
                }
            )
            if "Item" not in mlp_response:
                raise KeyError(
                    f"MLP config linkage missing for ml_project_name={ml_project_name}; required reciprocal mapping to project_name"
                )

            linked_project_name = str(mlp_response["Item"].get("project_name", "")).strip()
            if linked_project_name != project_name:
                raise ValueError(
                    "Reciprocal linkage mismatch: "
                    f"project_name={project_name} points to ml_project_name={ml_project_name}, "
                    f"but mlp_config maps back to project_name={linked_project_name}"
                )


def load_project_parameters(
    project_name: str,
    feature_spec_version: str,
    dpp_table_name: str | None = None,
    job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
) -> Dict[str, Any]:
    loader = ProjectParametersLoader(dpp_table_name=dpp_table_name)
    return loader.load(project_name=project_name, feature_spec_version=feature_spec_version, job_name=job_name)


def resolve_feature_spec_version(
    project_name: str,
    preferred_feature_spec_version: str | None = None,
    dpp_table_name: str | None = None,
    job_name: str = DEFAULT_PROJECT_PARAMETERS_JOB_NAME,
) -> str:
    loader = ProjectParametersLoader(dpp_table_name=dpp_table_name)

    if preferred_feature_spec_version:
        loader.load(
            project_name=project_name,
            feature_spec_version=preferred_feature_spec_version,
            job_name=job_name,
        )
        return preferred_feature_spec_version

    response = loader._dpp_table.query(
        KeyConditionExpression=Key("project_name").eq(project_name)
        & Key("job_name_version").begins_with(f"{job_name}{JOB_SPEC_SORT_KEY_DELIMITER}"),
    )
    items = response.get("Items", [])
    if not items:
        raise KeyError(f"No project_parameters records found for project={project_name}")

    def _item_rank(item: Dict[str, Any]) -> tuple[str, str]:
        updated_at = str(item.get("updated_at", ""))
        item_job_name = str(item.get("job_name_version") or "")
        return (updated_at, item_job_name)

    best = sorted(items, key=_item_rank, reverse=True)[0]
    job_name_value = str(best.get("job_name_version"))
    _, feature_spec_version = job_name_value.split(JOB_SPEC_SORT_KEY_DELIMITER, 1)
    return feature_spec_version


def resolve_dpp_config_table_name(table_name: str | None = None) -> str:
    return table_name or os.environ.get(DPP_CONFIG_TABLE_ENV_VAR) or DEFAULT_DPP_CONFIG_TABLE_NAME


def resolve_mlp_config_table_name(table_name: str | None = None) -> str:
    return table_name or os.environ.get(MLP_CONFIG_TABLE_ENV_VAR) or DEFAULT_MLP_CONFIG_TABLE_NAME


def resolve_batch_index_table_name(table_name: str | None = None) -> str:
    return table_name or os.environ.get(BATCH_INDEX_TABLE_ENV_VAR) or DEFAULT_BATCH_INDEX_TABLE_NAME
