import sys
import types
from pathlib import Path

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *_args, **_kwargs: None
sys.modules.setdefault("boto3", boto3_stub)

from ndr.processing.backfill_redshift_fallback import (
    BackfillFallbackRedshiftConfig,
    BackfillFlowQuerySpec,
    BackfillRange,
    build_unload_sql,
    execute_backfill_redshift_fallback,
    load_backfill_fallback_contract,
    resolve_backfill_source_mode,
    stage_unload_parquet_to_local,
)


class _S3Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, Bucket, Prefix):
        assert Bucket == "bucket"
        assert Prefix.startswith("tmp/backfill")
        return self._pages


class _S3Client:
    def __init__(self):
        self.download_calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _S3Paginator(
            [
                {
                    "Contents": [
                        {"Key": "tmp/backfill/range_0000/attempt_01/part-000.parquet"},
                        {"Key": "tmp/backfill/range_0000/attempt_01/_SUCCESS"},
                    ]
                }
            ]
        )

    def download_file(self, bucket, key, dest):
        self.download_calls.append((bucket, key, dest))
        Path(dest).write_bytes(b"parquet")


class _DataApi:
    def __init__(self):
        self.execute_sql = []
        self._statuses = {}

    def execute_statement(self, **kwargs):
        idx = len(self.execute_sql)
        self.execute_sql.append(kwargs["Sql"])
        statement_id = f"stmt-{idx}"
        # first attempt for first range fails, second attempt succeeds, all other succeed
        if idx == 0:
            self._statuses[statement_id] = [
                {"Status": "FAILED", "Error": "simulated"},
            ]
        else:
            self._statuses[statement_id] = [{"Status": "FINISHED"}]
        return {"Id": statement_id}

    def describe_statement(self, Id):
        statuses = self._statuses[Id]
        return statuses.pop(0)


def test_contract_rejects_generic_query_fallback():
    spec = {
        "backfill_redshift_fallback": {
            "redshift": {
                "cluster_identifier": "cluster",
                "database": "db",
                "secret_arn": "arn:secret",
                "region": "us-east-1",
                "iam_role": "arn:role",
                "unload_s3_prefix": "s3://bucket/tmp/backfill",
            },
            "queries": {
                "delta": {"descriptor_id": "d1", "sql_template": "select * from t"},
            },
        }
    }

    try:
        load_backfill_fallback_contract(dpp_spec=spec, artifact_family="fg_c")
    except ValueError as exc:
        assert "Generic query fallback is not allowed" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_ingestion_miss_resolves_to_redshift_fallback_mode():
    mode = resolve_backfill_source_mode(ingestion_rows=[], allow_redshift_fallback=True)
    assert mode == "redshift_unload_fallback"


def test_multi_range_execution_is_deterministic_with_retry_and_local_staging(monkeypatch, tmp_path):
    import ndr.processing.backfill_redshift_fallback as module

    fake_data_api = _DataApi()
    fake_s3 = _S3Client()

    def _client(name, **_kwargs):
        if name == "redshift-data":
            return fake_data_api
        if name == "s3":
            return fake_s3
        raise AssertionError(name)

    monkeypatch.setattr(module.boto3, "client", _client, raising=False)
    monkeypatch.setattr(module.time, "sleep", lambda *_a, **_k: None)

    config = BackfillFallbackRedshiftConfig(
        cluster_identifier="cluster",
        database="db",
        secret_arn="arn:secret",
        region="us-east-1",
        iam_role="arn:role",
        unload_s3_prefix="s3://bucket/tmp/backfill",
    )
    query = BackfillFlowQuerySpec(
        descriptor_id="delta_fallback_v1",
        sql_template="select * from src where event_ts >= '{{start_ts}}' and event_ts < '{{end_ts}}'",
    )
    ranges = [
        BackfillRange(start_ts="2025-01-01T01:00:00Z", end_ts="2025-01-01T01:15:00Z"),
        BackfillRange(start_ts="2025-01-01T00:00:00Z", end_ts="2025-01-01T00:15:00Z"),
    ]

    results = execute_backfill_redshift_fallback(
        config=config,
        query_spec=query,
        ranges=ranges,
        max_attempts=2,
        retry_backoff_seconds=0,
        local_staging_root=str(tmp_path),
    )

    assert len(results) == 2
    # sorted by start_ts regardless of input ordering
    assert results[0].range_start_ts == "2025-01-01T00:00:00Z"
    # first range needed a retry -> attempt 2
    assert results[0].attempt == 2
    assert results[1].attempt == 1
    assert all(result.local_files for result in results)


def test_stage_unload_keeps_only_parquet_files(tmp_path):
    s3 = _S3Client()
    files = stage_unload_parquet_to_local(
        s3_client=s3,
        unload_s3_prefix="s3://bucket/tmp/backfill/range_0000/attempt_01/",
        local_dir=str(tmp_path),
    )
    assert len(files) == 1
    assert files[0].endswith("part-000.parquet")


def test_build_unload_sql_is_unload_only():
    sql = build_unload_sql(
        select_sql="select * from src",
        unload_s3_prefix="s3://bucket/tmp/backfill/range_0000/attempt_01/",
        iam_role="arn:role",
    )
    assert "UNLOAD" in sql
    assert "FORMAT AS PARQUET" in sql
