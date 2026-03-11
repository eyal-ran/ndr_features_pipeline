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

    def query(self, **kwargs):
        self.query_calls.append(kwargs)
        if kwargs.get("IndexName") == "GSI1":
            return {
                "Items": [
                    {
                        "project_name": "fw_paloalto",
                        "data_source_name": "fw_paloalto",
                        "version": "v1",
                        "batch_id": "mb-2",
                        "raw_parsed_logs_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-2/",
                        "event_ts_utc": "2025-01-01T10:30:00Z",
                    }
                ]
            }

        return {
            "Items": [
                {
                    "project_name": "fw_paloalto",
                    "data_source_name": "fw_paloalto",
                    "version": "v1",
                    "batch_id": "mb-1",
                    "raw_parsed_logs_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-1/",
                    "event_ts_utc": "2025-01-01T10:15:00Z",
                },
                {
                    "project_name": "fw_paloalto",
                    "data_source_name": "fw_paloalto",
                    "version": "v1",
                    "batch_id": "mb-2",
                    "raw_parsed_logs_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-2/",
                    "event_ts_utc": "2025-01-01T10:30:00Z",
                },
            ]
        }


class _Resource:
    def __init__(self, table):
        self.table = table

    def Table(self, *_args, **_kwargs):
        return self.table


def test_lookup_forward_filters_by_time_window(monkeypatch):
    import ndr.config.batch_index_loader as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    loader = BatchIndexLoader(table_name="idx")
    rows = loader.lookup_forward(
        project_name="fw_paloalto",
        data_source_name="fw_paloalto",
        version="v1",
        start_ts_iso="2025-01-01T10:20:00Z",
        end_ts_iso="2025-01-01T11:00:00Z",
    )
    assert len(rows) == 1
    assert rows[0].batch_id == "mb-2"


def test_lookup_reverse_uses_gsi1(monkeypatch):
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


def test_lookup_forward_handles_paginated_query(monkeypatch):
    import ndr.config.batch_index_loader as module

    class _PagedTable(_Table):
        def query(self, **kwargs):
            self.query_calls.append(kwargs)
            if "ExclusiveStartKey" not in kwargs:
                return {
                    "Items": [
                        {
                            "project_name": "fw_paloalto",
                            "data_source_name": "fw_paloalto",
                            "version": "v1",
                            "batch_id": "mb-1",
                            "batch_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-1/",
                            "event_ts_utc": "2025-01-01T10:10:00Z",
                        }
                    ],
                    "LastEvaluatedKey": {"pk": "x", "sk": "y"},
                }
            return {
                "Items": [
                    {
                        "project_name": "fw_paloalto",
                        "data_source_name": "fw_paloalto",
                        "version": "v1",
                        "batch_id": "mb-2",
                        "raw_parsed_logs_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-2/",
                        "event_ts_utc": "2025-01-01T10:20:00Z",
                    }
                ]
            }

    fake_table = _PagedTable()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    loader = BatchIndexLoader(table_name="idx")
    rows = loader.lookup_forward(
        project_name="fw_paloalto",
        data_source_name="fw_paloalto",
        version="v1",
        start_ts_iso="2025-01-01T10:00:00Z",
        end_ts_iso="2025-01-01T11:00:00Z",
    )
    assert [row.batch_id for row in rows] == ["mb-1", "mb-2"]


def test_lookup_forward_tolerates_legacy_batch_s3_prefix(monkeypatch):
    import ndr.config.batch_index_loader as module

    class _LegacyTable(_Table):
        def query(self, **kwargs):
            self.query_calls.append(kwargs)
            return {
                "Items": [
                    {
                        "project_name": "fw_paloalto",
                        "data_source_name": "fw_paloalto",
                        "version": "v1",
                        "batch_id": "mb-3",
                        "batch_s3_prefix": "s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-3/",
                        "event_ts_utc": "2025-01-01T10:30:00Z",
                    }
                ]
            }

    fake_table = _LegacyTable()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    loader = BatchIndexLoader(table_name="idx")
    rows = loader.lookup_forward(
        project_name="fw_paloalto",
        data_source_name="fw_paloalto",
        version="v1",
        start_ts_iso="2025-01-01T10:00:00Z",
        end_ts_iso="2025-01-01T11:00:00Z",
    )
    assert rows[0].raw_parsed_logs_s3_prefix.endswith("/mb-3/")
