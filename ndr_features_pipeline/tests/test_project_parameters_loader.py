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

from ndr.config.project_parameters_loader import resolve_feature_spec_version


class DummyTable:
    def query(self, **kwargs):
        return {
            "Items": [
                {"job_name": "project_parameters#v1", "updated_at": "2025-01-01T00:00:00Z"},
                {"job_name": "project_parameters#v2", "updated_at": "2025-02-01T00:00:00Z"},
            ]
        }

    def get_item(self, Key):
        return {"Item": {"spec": {"ok": True}}}


class DummyResource:
    def Table(self, name):
        return DummyTable()


def test_resolve_feature_spec_version_uses_latest(monkeypatch):
    import ndr.config.project_parameters_loader as module

    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: DummyResource(), raising=False)
    out = resolve_feature_spec_version(project_name="proj1", table_name="dummy")
    assert out == "v2"
