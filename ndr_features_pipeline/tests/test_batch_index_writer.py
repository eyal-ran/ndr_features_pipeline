import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules["boto3"] = boto3_stub

exceptions_module = types.ModuleType("botocore.exceptions")


class ClientError(Exception):
    def __init__(self, response, operation_name):
        super().__init__(operation_name)
        self.response = response


exceptions_module.ClientError = ClientError
sys.modules["botocore.exceptions"] = exceptions_module

from ndr.config.batch_index_writer import BatchIndexWriteRequest, BatchIndexWriter


class _Table:
    def __init__(self):
        self.put_calls = []
        self.update_calls = []

    def put_item(self, **kwargs):
        self.put_calls.append(kwargs)
        raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)


class _Resource:
    def __init__(self, table):
        self.table = table

    def Table(self, *_args, **_kwargs):
        return self.table


def test_upsert_uses_exact_idempotent_contract(monkeypatch):
    import ndr.config.batch_index_writer as module

    fake_table = _Table()
    monkeypatch.setattr(module.boto3, "resource", lambda *_a, **_k: _Resource(fake_table), raising=False)

    writer = BatchIndexWriter(table_name="idx")
    writer.upsert(
        BatchIndexWriteRequest(
            pk="fw_paloalto#fw_paloalto#v1#2025-01-01",
            sk="10#2#mb-1",
            project_name="fw_paloalto",
            data_source_name="fw_paloalto",
            version="v1",
            date_utc="2025-01-01",
            hour_utc="10",
            slot15=2,
            batch_id="mb-1",
            raw_parsed_logs_s3_prefix="s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-1/",
            event_ts_utc="2025-01-01T10:20:00Z",
            org1="org1",
            org2="org2",
            status="RECEIVED",
            gsi1pk="fw_paloalto#fw_paloalto#v1#mb-1",
            gsi1sk="2025-01-01T10:20:00Z",
            ml_project_name="network_anomalies_detection",
        )
    )

    assert fake_table.put_calls
    assert (
        fake_table.put_calls[0]["ConditionExpression"]
        == "attribute_not_exists(pk) AND attribute_not_exists(sk)"
    )
    assert fake_table.update_calls
    update_call = fake_table.update_calls[0]
    assert update_call["ConditionExpression"] == "attribute_exists(pk) AND attribute_exists(sk)"
    assert "raw_parsed_logs_s3_prefix = :raw_parsed_logs_s3_prefix" in update_call["UpdateExpression"]
    assert "ml_project_name = if_not_exists(ml_project_name, :ml_project_name)" in update_call["UpdateExpression"]
