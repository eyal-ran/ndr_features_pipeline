import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules["boto3"] = boto3_stub

conditions_module = types.ModuleType("boto3.dynamodb.conditions")


class _Expr:
    def __init__(self, *args, **kwargs):
        pass

    def __and__(self, other):
        return self


class _Key:
    def __init__(self, name):
        self.name = name

    def eq(self, val):
        return _Expr(self.name, "eq", val)

    def begins_with(self, val):
        return _Expr(self.name, "begins_with", val)


conditions_module.Key = _Key
sys.modules["boto3.dynamodb.conditions"] = conditions_module

from ndr.config.project_parameters_loader import (
    resolve_batch_index_table_name,
    resolve_feature_spec_version,
    load_project_parameters,
)


class DummyDppTable:
    def query(self, **kwargs):
        return {
            "Items": [
                {"job_name_version": "project_parameters#v1", "updated_at": "2025-01-01T00:00:00Z"},
                {"job_name_version": "project_parameters#v2", "updated_at": "2025-02-01T00:00:00Z"},
            ]
        }

    def get_item(self, Key):
        return {
            "Item": {
                "project_name": "proj1",
                "job_name_version": "project_parameters#v2",
                "ml_project_name": "ml-proj1",
                "spec": {"ok": True},
            }
        }


class DummyMlpTable:
    def get_item(self, Key):
        return {"Item": {"project_name": "proj1"}}


class DummyResource:
    def Table(self, name):
        if name == "dpp-table":
            return DummyDppTable()
        return DummyMlpTable()


def test_resolve_feature_spec_version_uses_latest(monkeypatch):
    import ndr.config.project_parameters_loader as module

    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: DummyResource(), raising=False)
    out = resolve_feature_spec_version(project_name="proj1", dpp_table_name="dpp-table")
    assert out == "v2"


def test_load_project_parameters_enforces_reciprocal_linkage(monkeypatch):
    import ndr.config.project_parameters_loader as module

    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: DummyResource(), raising=False)
    out = load_project_parameters(project_name="proj1", feature_spec_version="v2", dpp_table_name="dpp-table")
    assert out == {"ok": True}


def test_resolve_batch_index_table_name_prefers_canonical_env(monkeypatch):
    monkeypatch.setenv("BATCH_INDEX_TABLE_NAME", "idx-table")
    assert resolve_batch_index_table_name() == "idx-table"


def test_resolve_table_names_ignore_legacy_env_vars(monkeypatch):
    from ndr.config.project_parameters_loader import resolve_dpp_config_table_name, resolve_batch_index_table_name

    monkeypatch.delenv("DPP_CONFIG_TABLE_NAME", raising=False)
    monkeypatch.delenv("BATCH_INDEX_TABLE_NAME", raising=False)
    monkeypatch.setenv("ML_PROJECTS_PARAMETERS_TABLE_NAME", "legacy")
    monkeypatch.setenv("JOB_SPEC_DDB_TABLE_NAME", "legacy2")
    monkeypatch.setenv("NDR_BATCH_INDEX_TABLE_NAME", "legacy3")
    monkeypatch.setenv("BATCH_INDEX_DDB_TABLE_NAME", "legacy4")

    assert resolve_dpp_config_table_name() == "dpp_config"
    assert resolve_batch_index_table_name() == "batch_index"
