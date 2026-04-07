import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules["boto3"] = boto3_stub

from ndr.config.batch_index_writer import BatchIndexWriteRequest, BatchIndexWriter


class _Table:
    def __init__(self):
        self.put_calls = []

    def put_item(self, **kwargs):
        self.put_calls.append(kwargs)


class _Resource:
    def __init__(self, table):
        self.table = table

    def Table(self, *_args, **_kwargs):
        return self.table


def test_upsert_writes_dual_items(monkeypatch):
    import ndr.config.batch_index_writer as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)

    writer = BatchIndexWriter(table_name="idx")
    writer.upsert_dual_items(
        BatchIndexWriteRequest(
            project_name="fw_paloalto",
            batch_id="mb-1",
            date_partition="2025/01/01",
            hour="10",
            within_hour_run_number="2",
            etl_ts="2025-01-01T10:20:00Z",
            org1="org1",
            org2="org2",
            raw_parsed_logs_s3_prefix="s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-1/",
            ml_project_names=["ml-a"],
            s3_prefixes={"dpp": {"delta": "s3://x"}, "mlp": {"ml-a": {"predictions": "s3://y"}}},
        )
    )

    assert len(fake_table.put_calls) == 2
    batch_item = fake_table.put_calls[0]["Item"]
    date_item = fake_table.put_calls[1]["Item"]
    assert batch_item["PK"] == "fw_paloalto"
    assert batch_item["SK"] == "mb-1"
    assert date_item["SK"] == "2025/01/01#10#2"
    assert date_item["batch_lookup_sk"] == "mb-1"
