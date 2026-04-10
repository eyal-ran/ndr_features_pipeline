import sys
import types

sys.modules.setdefault("boto3", types.ModuleType("boto3"))
boto3_dynamodb = types.ModuleType("boto3.dynamodb")
boto3_dynamodb_conditions = types.ModuleType("boto3.dynamodb.conditions")
boto3_dynamodb_conditions.Key = object
sys.modules.setdefault("boto3.dynamodb", boto3_dynamodb)
sys.modules.setdefault("boto3.dynamodb.conditions", boto3_dynamodb_conditions)

pyspark_module = types.ModuleType("pyspark")
pyspark_sql_module = types.ModuleType("pyspark.sql")
pyspark_sql_module.DataFrame = object
pyspark_sql_module.SparkSession = object
pyspark_sql_functions_module = types.ModuleType("pyspark.sql.functions")
pyspark_sql_module.functions = pyspark_sql_functions_module
sys.modules.setdefault("pyspark", pyspark_module)
sys.modules.setdefault("pyspark.sql", pyspark_sql_module)
sys.modules.setdefault("pyspark.sql.functions", pyspark_sql_functions_module)

from ndr.processing.machine_inventory_unload_job import (
    MachineInventoryOutputSpec,
    MachineInventoryQuerySpec,
    MachineInventoryUnloadJob,
    MachineInventoryUnloadRuntimeConfig,
    RedshiftDataApiConfig,
    build_snapshot_month,
    build_snapshot_output_prefix,
    build_unload_staging_prefix,
    build_source_query,
    build_unload_sql,
    parse_machine_inventory_spec,
)


def test_build_snapshot_month():
    assert build_snapshot_month("2025/12") == "2025-12"


def test_build_snapshot_output_prefix():
    assert (
        build_snapshot_output_prefix("s3://bucket/prefix", "2025-12")
        == "s3://bucket/prefix/snapshot_month=2025-12/"
    )


def test_build_unload_staging_prefix():
    assert (
        build_unload_staging_prefix("s3://bucket/prefix", "2025-12", "run123")
        == "s3://bucket/prefix/snapshot_month=2025-12/_tmp_run_id=run123/"
    )


def test_month_partition_prefix_is_deterministic_for_replay():
    month = build_snapshot_month("2025/12")
    assert build_snapshot_output_prefix("s3://bucket/prefix", month) == (
        "s3://bucket/prefix/snapshot_month=2025-12/"
    )
    assert build_snapshot_output_prefix("s3://bucket/prefix/", month) == (
        "s3://bucket/prefix/snapshot_month=2025-12/"
    )


def test_month_over_month_partition_prefixes_remain_distinct():
    dec = build_snapshot_output_prefix("s3://bucket/prefix", build_snapshot_month("2025/12"))
    jan = build_snapshot_output_prefix("s3://bucket/prefix", build_snapshot_month("2026/01"))
    assert dec != jan
    assert dec.endswith("snapshot_month=2025-12/")
    assert jan.endswith("snapshot_month=2026-01/")


def test_build_source_query_with_descriptor_sql():
    query_spec = MachineInventoryQuerySpec(
        sql="select ip_addr, host_name from public.dim_machine where is_active = true and region = 'us-east-1'",
        descriptor_id="monthly_inventory_v1",
        ip_column="ip_addr",
        name_column="host_name",
        active_filter=None,
        additional_filters=[],
    )
    sql = build_source_query(query_spec)
    assert "FROM (select ip_addr, host_name from public.dim_machine" in sql
    assert "is_active = true" in sql
    assert "region = 'us-east-1'" in sql


def test_build_source_query_with_sql_override():
    query_spec = MachineInventoryQuerySpec(
        sql="select ip_addr, host_name from public.dim_machine where is_active = true",
        descriptor_id="monthly_inventory_v1",
        ip_column="ip_addr",
        name_column="host_name",
        active_filter=None,
        additional_filters=[],
    )
    sql = build_source_query(query_spec)
    assert sql.startswith("SELECT ip_addr AS ip_address")


def test_build_unload_sql():
    sql = build_unload_sql(
        filtered_query="select 1",
        output_prefix="s3://bucket/prefix/",
        output_format="PARQUET",
        iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
    )
    assert "UNLOAD" in sql
    assert "FORMAT AS PARQUET" in sql


def test_parse_machine_inventory_spec_requires_iam_role():
    job_spec = {
        "redshift": {
            "cluster_identifier": "cluster",
            "database": "db",
            "secret_arn": "arn:secret",
            "region": "us-east-1",
        },
        "query": {"sql": "select ip_address, machine_name from dim_machine"},
        "output": {"s3_prefix": "s3://bucket/prefix/"},
    }
    try:
        parse_machine_inventory_spec(job_spec)
    except ValueError as exc:
        assert "iam_role" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing iam_role")


def test_parse_machine_inventory_spec_requires_query_sql():
    job_spec = {
        "redshift": {
            "cluster_identifier": "cluster",
            "database": "db",
            "secret_arn": "arn:secret",
            "region": "us-east-1",
            "iam_role": "arn:aws:iam::123456789012:role/RedshiftRole",
        },
        "query": {},
        "output": {"s3_prefix": "s3://bucket/prefix/"},
    }
    try:
        parse_machine_inventory_spec(job_spec)
    except ValueError as exc:
        assert "query.sql" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing query.sql")


def test_parse_machine_inventory_spec_requires_mapping_contract_version():
    job_spec = {
        "redshift": {
            "cluster_identifier": "cluster",
            "database": "db",
            "secret_arn": "arn:secret",
            "region": "us-east-1",
            "iam_role": "arn:aws:iam::123456789012:role/RedshiftRole",
        },
        "query": {"sql": "select ip_address, machine_name from dim_machine"},
        "output": {"s3_prefix": "s3://bucket/prefix/", "mapping_contract_version": ""},
    }
    try:
        parse_machine_inventory_spec(job_spec)
    except ValueError as exc:
        assert "mapping_contract_version" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing mapping_contract_version")


class _StubDataApi:
    def __init__(self, count_records=None):
        self._count_records = [[{"longValue": 2}]] if count_records is None else count_records
        self.get_statement_result_calls = []

    def get_statement_result(self, **kwargs):
        self.get_statement_result_calls.append(kwargs)
        return {"Records": self._count_records}


def _build_job():
    return MachineInventoryUnloadJob(
        spark=None,
        job_spec={},
        runtime_config=MachineInventoryUnloadRuntimeConfig(
            project_name="proj",
            feature_spec_version="v1",
            reference_month="2025/12",
        ),
    )


def _build_redshift_cfg():
    return RedshiftDataApiConfig(
        cluster_identifier="cluster",
        database="db",
        secret_arn="arn:secret",
        region="us-east-1",
        iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
    )


def test_unload_from_redshift_uses_full_snapshot_query(monkeypatch):
    job = _build_job()
    data_api = _StubDataApi()
    captured_sql = []

    def _fake_execute_statement(data_api_client, config, sql):
        captured_sql.append(sql)
        if sql.startswith("UNLOAD"):
            return "unload_stmt"
        return "count_stmt"

    monkeypatch.setattr(job, "_execute_statement", _fake_execute_statement)
    monkeypatch.setattr(job, "_get_unload_row_count", lambda *_args, **_kwargs: 2)
    source_rows, unloaded_rows = job._unload_from_redshift(
        data_api=data_api,
        redshift_cfg=_build_redshift_cfg(),
        query_spec=MachineInventoryQuerySpec(
            sql="select ip_address, machine_name from dim_machine where is_active = true",
            descriptor_id="machine_inventory_monthly_v1",
            ip_column="ip_address",
            name_column="machine_name",
            active_filter=None,
            additional_filters=[],
        ),
        output_spec=MachineInventoryOutputSpec(
            s3_prefix="s3://bucket/prefix/",
            output_format="PARQUET",
            partitioning=["snapshot_month"],
        ),
        output_prefix="s3://bucket/prefix/snapshot_month=2025-12/_tmp_run_id=abc/",
    )

    assert source_rows == 2
    assert unloaded_rows == 2
    assert any("SELECT COUNT(*) AS row_count" in sql for sql in captured_sql)
    assert any("UNLOAD" in sql for sql in captured_sql)
    assert not any("tmp_existing_ips" in sql for sql in captured_sql)


def test_get_query_row_count_raises_on_empty_result(monkeypatch):
    job = _build_job()
    data_api = _StubDataApi(count_records=[])
    monkeypatch.setattr(job, "_execute_statement", lambda *_args, **_kwargs: "stmt")

    try:
        job._get_query_row_count(
            data_api=data_api,
            config=_build_redshift_cfg(),
            query="select 1",
        )
    except RuntimeError as exc:
        assert "MACHINE_INVENTORY_SOURCE_COUNT_FAILED" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for empty source count result")


def test_update_canonical_mapping_pointer_updates_current_and_previous(monkeypatch):
    class _StubTable:
        def __init__(self):
            self.item = {
                "project_name": "proj",
                "job_name_version": "project_parameters#v1",
                "spec": {"ip_machine_mapping_s3_prefix": "s3://bucket/prefix/snapshot_month=2025-11/"},
            }
            self.put_item_payload = None

        def get_item(self, **_kwargs):
            return {"Item": dict(self.item)}

        def put_item(self, **kwargs):
            self.put_item_payload = kwargs["Item"]

    class _StubDdb:
        def __init__(self, table):
            self._table = table

        def Table(self, _name):
            return self._table

    table = _StubTable()
    monkeypatch.setattr("ndr.processing.machine_inventory_unload_job.resolve_dpp_config_table_name", lambda _name=None: "dpp_config")
    monkeypatch.setattr(
        "ndr.processing.machine_inventory_unload_job.boto3.resource",
        lambda _service: _StubDdb(table),
        raising=False,
    )

    job = _build_job()
    job._update_canonical_mapping_pointer(
        partition_prefix="s3://bucket/prefix/snapshot_month=2025-12/",
        snapshot_month="2025-12",
    )

    assert table.put_item_payload["spec"]["ip_machine_mapping_s3_prefix"] == "s3://bucket/prefix/snapshot_month=2025-12/"
    assert table.put_item_payload["spec"]["ip_machine_mapping_previous_s3_prefix"] == "s3://bucket/prefix/snapshot_month=2025-11/"
