import sys
import types

sys.modules.setdefault("boto3", types.ModuleType("boto3"))

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
    MachineInventoryQuerySpec,
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
