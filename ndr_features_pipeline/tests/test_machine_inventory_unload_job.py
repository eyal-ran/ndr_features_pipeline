from ndr.processing.machine_inventory_unload_job import (
    MachineInventoryQuerySpec,
    build_snapshot_month,
    build_snapshot_output_prefix,
    build_temp_output_prefix,
    build_source_query,
    build_unload_sql,
    parse_machine_inventory_spec,
)


def test_build_snapshot_month():
    assert build_snapshot_month("2025-12-01T00:00:00Z") == "2025-12"


def test_build_snapshot_output_prefix():
    assert (
        build_snapshot_output_prefix("s3://bucket/prefix", "2025-12")
        == "s3://bucket/prefix/snapshot_month=2025-12/"
    )


def test_build_temp_output_prefix():
    assert (
        build_temp_output_prefix("s3://bucket/prefix", "2025-12", "run123")
        == "s3://bucket/prefix/snapshot_month=2025-12/_tmp_run_id=run123/"
    )


def test_build_source_query_with_structured_fields():
    query_spec = MachineInventoryQuerySpec(
        sql=None,
        schema="public",
        table="dim_machine",
        ip_column="ip_addr",
        name_column="host_name",
        active_filter="is_active = true",
        additional_filters=["region = 'us-east-1'"],
    )
    sql = build_source_query(query_spec)
    assert "FROM public.dim_machine" in sql
    assert "is_active = true" in sql
    assert "region = 'us-east-1'" in sql


def test_build_source_query_with_sql_override():
    query_spec = MachineInventoryQuerySpec(
        sql="select ip_addr, host_name from public.dim_machine where is_active = true",
        schema=None,
        table=None,
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
        "query": {"schema": "public", "table": "dim_machine"},
        "output": {"s3_prefix": "s3://bucket/prefix/"},
    }
    try:
        parse_machine_inventory_spec(job_spec)
    except ValueError as exc:
        assert "iam_role" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing iam_role")
