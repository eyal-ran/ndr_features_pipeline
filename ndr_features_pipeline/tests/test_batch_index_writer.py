import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules["boto3"] = boto3_stub
conditions_module = types.ModuleType("boto3.dynamodb.conditions")
conditions_module.Key = object
sys.modules["boto3.dynamodb.conditions"] = conditions_module

from ndr.config.batch_index_writer import BatchIndexWriteRequest, BatchIndexWriter


class _Table:
    def __init__(self):
        self.put_calls = []
        self.update_calls = []
        self.fail_first_put_keys = set()

    def put_item(self, **kwargs):
        item = kwargs["Item"]
        key = (item["PK"], item["SK"])
        if key in self.fail_first_put_keys:
            self.fail_first_put_keys.remove(key)
            raise _ConditionalWriteError()
        self.put_calls.append(kwargs)

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)


class _Resource:
    def __init__(self, table):
        self.table = table

    def Table(self, *_args, **_kwargs):
        return self.table


class _ConditionalWriteError(Exception):
    def __init__(self):
        super().__init__("ConditionalCheckFailedException")
        self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}


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
    assert fake_table.put_calls[0]["ConditionExpression"] == "attribute_not_exists(PK) AND attribute_not_exists(SK)"


def test_upsert_is_idempotent_when_items_already_exist(monkeypatch):
    import ndr.config.batch_index_writer as module

    fake_table = _Table()
    fake_table.fail_first_put_keys = {("fw_paloalto", "mb-1"), ("fw_paloalto", "2025/01/01#10#2")}
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

    assert len(fake_table.put_calls) == 0
    assert len(fake_table.update_calls) == 2
    assert fake_table.update_calls[0]["ConditionExpression"] == "attribute_exists(PK) AND attribute_exists(SK)"


def test_status_updates_only_touch_status_and_timestamp(monkeypatch):
    import ndr.config.batch_index_writer as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)
    writer = BatchIndexWriter(table_name="idx")
    writer.update_status(project_name="fw_paloalto", batch_id="mb-1", rt_flow_status="RUNNING")

    assert len(fake_table.update_calls) == 1
    expression = fake_table.update_calls[0]["UpdateExpression"]
    assert "rt_flow_status" in expression
    assert "backfill_status" not in expression
