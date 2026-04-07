import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules["boto3"] = boto3_stub

conditions_module = types.ModuleType("boto3.dynamodb.conditions")


class _Expr:
    def __init__(self, *args, **kwargs):
        pass

    def __and__(self, _other):
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

from ndr.config.batch_index_loader import BatchIndexLoader


class _Table:
    def __init__(self):
        self.query_calls = []

    def get_item(self, **kwargs):
        if kwargs["Key"]["SK"] == "mb-2":
            return {
                "Item": {
                    "PK": "fw_paloalto",
                    "SK": "mb-2",
                    "batch_id": "mb-2",
                    "date_partition": "2025/01/01",
                    "hour": "10",
                    "within_hour_run_number": "2",
                    "etl_ts": "2025-01-01T10:30:00Z",
                    "org1": "org1",
                    "org2": "org2",
                    "raw_parsed_logs_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-2/",
                    "ml_project_names": ["ml-a"],
                    "s3_prefixes": {"mlp": {"ml-a": {"predictions": "s3://x"}}},
                }
            }
        return {}

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        return {
            "Items": [
                {
                    "PK": "fw_paloalto",
                    "SK": "2025/01/01#10#2",
                    "batch_id": "mb-2",
                    "batch_lookup_sk": "mb-2",
                    "date_partition": "2025/01/01",
                    "hour": "10",
                    "within_hour_run_number": "2",
                    "etl_ts": "2025-01-01T10:30:00Z",
                }
            ]
        }


class _Resource:
    def __init__(self, table):
        self.table = table

    def Table(self, *_args, **_kwargs):
        return self.table


def test_lookup_by_date_partition_returns_reverse_items(monkeypatch):
    import ndr.config.batch_index_loader as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    loader = BatchIndexLoader(table_name="idx")
    rows = loader.lookup_by_date_partition(project_name="fw_paloalto", date_partition="2025/01/01")
    assert len(rows) == 1
    assert rows[0]["batch_lookup_sk"] == "mb-2"


def test_lookup_reverse_reads_primary_key_not_gsi(monkeypatch):
    import ndr.config.batch_index_loader as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    loader = BatchIndexLoader(table_name="idx")
    row = loader.lookup_reverse(
        project_name="fw_paloalto",
        data_source_name="fw_paloalto",
        version="v1",
        batch_id="mb-2",
    )
    assert row is not None
    assert row.raw_parsed_logs_s3_prefix.endswith("/mb-2/")


def test_resolve_mlp_branch_prefixes(monkeypatch):
    import ndr.config.batch_index_loader as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    loader = BatchIndexLoader(table_name="idx")
    branch = loader.resolve_mlp_branch_prefixes(
        project_name="fw_paloalto",
        batch_id="mb-2",
        ml_project_name="ml-a",
    )
    assert branch["predictions"] == "s3://x"
