import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules.setdefault("boto3", boto3_stub)

from ndr.config.batch_index_writer import BatchIndexWriteRequest, BatchIndexWriter
from ndr.contracts import date_lookup_sk
from ndr.processing.backfill_batch_index_write_path import BackfillBatchIndexWritePath


class _ConditionalWriteError(Exception):
    def __init__(self):
        super().__init__("ConditionalCheckFailedException")
        self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class _FakeTable:
    def __init__(self):
        self.put_calls = []
        self.update_calls = []
        self.items = {}
        self.conditional_keys = set()
        self.raise_on_date_put_once = False

    def put_item(self, **kwargs):
        item = kwargs["Item"]
        key = (item["PK"], item["SK"])
        if key[1].count("#") == 2 and self.raise_on_date_put_once:
            self.raise_on_date_put_once = False
            raise RuntimeError("simulated-date-put-failure")
        if key in self.conditional_keys:
            raise _ConditionalWriteError()
        self.put_calls.append(kwargs)
        self.items[key] = dict(item)

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)
        key = (kwargs["Key"]["PK"], kwargs["Key"]["SK"])
        item = self.items.setdefault(key, {"PK": key[0], "SK": key[1]})
        values = kwargs.get("ExpressionAttributeValues", {})
        if ":backfill_status" in values:
            item["backfill_status"] = values[":backfill_status"]
        item["last_updated_at"] = values.get(":last_updated_at", item.get("last_updated_at"))

    def get_item(self, **kwargs):
        key = (kwargs["Key"]["PK"], kwargs["Key"]["SK"])
        if key in self.items:
            return {"Item": dict(self.items[key])}
        return {}


class _FakeResource:
    def __init__(self, table):
        self._table = table

    def Table(self, *_args, **_kwargs):
        return self._table


def _request() -> BatchIndexWriteRequest:
    return BatchIndexWriteRequest(
        project_name="fw_paloalto",
        batch_id="mb-1",
        date_partition="2025/01/01",
        hour="10",
        within_hour_run_number="2",
        etl_ts="2025-01-01T10:20:00Z",
        org1="org1",
        org2="org2",
        raw_parsed_logs_s3_prefix="s3://bucket/fw_paloalto/org1/org2/2025/01/01/mb-1/",
        ml_project_names=["ml-a", "ml-b"],
        s3_prefixes={
            "dpp": {"delta": "s3://x"},
            "mlp": {
                "ml-a": {"predictions": "s3://y"},
                "ml-b": {"predictions": "s3://z"},
            },
        },
    )


def test_dual_item_write_and_status_progression(monkeypatch):
    import ndr.config.batch_index_writer as writer_module

    table = _FakeTable()
    monkeypatch.setattr(writer_module.boto3, "resource", lambda *_a, **_k: _FakeResource(table), raising=False)

    path = BackfillBatchIndexWritePath(writer=BatchIndexWriter(table_name="idx"))
    result = path.materialize_reconstructed_batch(write_request=_request(), idempotency_token="token-1")

    date_sk = date_lookup_sk(date_partition="2025/01/01", hour="10", within_hour_run_number="2")
    assert ("fw_paloalto", "mb-1") in table.items
    assert ("fw_paloalto", date_sk) in table.items
    assert result.final_status == "published"
    transitions = [
        call["ExpressionAttributeValues"].get(":backfill_status")
        for call in table.update_calls
        if ":backfill_status" in call["ExpressionAttributeValues"]
    ]
    assert transitions[:4] == ["planned", "materialized", "validated", "published"]


def test_idempotent_rerun_keeps_dual_items_with_conditional_write_safety(monkeypatch):
    import ndr.config.batch_index_writer as writer_module

    table = _FakeTable()
    date_sk = date_lookup_sk(date_partition="2025/01/01", hour="10", within_hour_run_number="2")
    table.conditional_keys = {("fw_paloalto", "mb-1"), ("fw_paloalto", date_sk)}
    table.items[("fw_paloalto", "mb-1")] = {"PK": "fw_paloalto", "SK": "mb-1"}
    table.items[("fw_paloalto", date_sk)] = {"PK": "fw_paloalto", "SK": date_sk}
    monkeypatch.setattr(writer_module.boto3, "resource", lambda *_a, **_k: _FakeResource(table), raising=False)

    path = BackfillBatchIndexWritePath(writer=BatchIndexWriter(table_name="idx"))
    result = path.materialize_reconstructed_batch(write_request=_request(), idempotency_token="token-2")

    assert result.final_status == "published"
    assert len(table.update_calls) >= 6  # two conditional-safe upserts + status progression updates


def test_partial_write_reconciliation_recovers_missing_item(monkeypatch):
    import ndr.config.batch_index_writer as writer_module

    table = _FakeTable()
    table.raise_on_date_put_once = True
    monkeypatch.setattr(writer_module.boto3, "resource", lambda *_a, **_k: _FakeResource(table), raising=False)

    path = BackfillBatchIndexWritePath(writer=BatchIndexWriter(table_name="idx"))
    result = path.materialize_reconstructed_batch(write_request=_request(), idempotency_token="token-3")

    date_sk = date_lookup_sk(date_partition="2025/01/01", hour="10", within_hour_run_number="2")
    assert result.final_status == "failed"
    assert result.reconciled is True
    assert ("fw_paloalto", "mb-1") in table.items
    assert ("fw_paloalto", date_sk) in table.items


def test_branch_level_prefix_coverage_is_required(monkeypatch):
    import ndr.config.batch_index_writer as writer_module

    table = _FakeTable()
    monkeypatch.setattr(writer_module.boto3, "resource", lambda *_a, **_k: _FakeResource(table), raising=False)

    req = _request()
    req.s3_prefixes["mlp"].pop("ml-b")
    path = BackfillBatchIndexWritePath(writer=BatchIndexWriter(table_name="idx"))

    try:
        path.materialize_reconstructed_batch(write_request=req, idempotency_token="token-4")
    except ValueError as exc:
        assert "missing branch entries" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
