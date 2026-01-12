
"""Unit tests for FGABuilderJob.

These tests focus on:
    - Correct window filtering and aggregation semantics.
    - Presence of key metadata and context columns.
The tests use a local SparkSession with an in-memory delta-like DataFrame.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession, functions as F

from ndr.processing.fg_a_builder_job import FGABuilderConfig, FGABuilderJob


def _create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("fg_a_builder_tests")
        .getOrCreate()
    )


def test_fg_a_builder_basic_window_aggregation(tmp_path):
    spark = _create_spark()

    # Build a tiny delta-like DataFrame with two slices for a single host.
    anchor = dt.datetime(2025, 1, 1, 12, 0, 0)
    slice1_end = anchor - dt.timedelta(minutes=15)
    slice2_end = anchor

    data = [
        {
            "mini_batch_id": "batch-1",
            "host_ip": "10.0.0.1",
            "role": "outbound",
            "slice_start_ts": slice1_end - dt.timedelta(minutes=15),
            "slice_end_ts": slice1_end,
            "sessions_cnt": 10,
            "allow_cnt": 8,
            "deny_cnt": 2,
            "alert_cnt": 0,
            "rst_cnt": 1,
            "aged_out_cnt": 0,
            "zero_reply_cnt": 1,
            "short_session_cnt": 2,
            "bytes_src_sum": 1000.0,
            "bytes_dst_sum": 500.0,
            "duration_sum": 30.0,
            "tcp_cnt": 10,
            "udp_cnt": 0,
            "icmp_cnt": 0,
            "threat_cnt": 0,
            "traffic_cnt": 10,
            "peer_ip_nunique": 3,
            "peer_port_nunique": 2,
            "peer_segment_nunique": 1,
            "peer_ip_entropy": 1.0,
            "peer_port_entropy": 0.5,
            "peer_ip_top1_sessions_share": 0.6,
            "peer_port_top1_sessions_share": 0.7,
            "peer_ip_top1_bytes_share": 0.5,
            "peer_port_top1_bytes_share": 0.5,
            "max_sessions_per_minute": 5,
            "high_risk_port_sessions_cnt": 1,
            "admin_port_sessions_cnt": 0,
            "fileshare_port_sessions_cnt": 0,
            "directory_port_sessions_cnt": 0,
            "db_port_sessions_cnt": 0,
        },
        {
            "mini_batch_id": "batch-1",
            "host_ip": "10.0.0.1",
            "role": "outbound",
            "slice_start_ts": slice2_end - dt.timedelta(minutes=15),
            "slice_end_ts": slice2_end,
            "sessions_cnt": 20,
            "allow_cnt": 15,
            "deny_cnt": 5,
            "alert_cnt": 1,
            "rst_cnt": 2,
            "aged_out_cnt": 1,
            "zero_reply_cnt": 2,
            "short_session_cnt": 3,
            "bytes_src_sum": 2000.0,
            "bytes_dst_sum": 1000.0,
            "duration_sum": 60.0,
            "tcp_cnt": 18,
            "udp_cnt": 2,
            "icmp_cnt": 0,
            "threat_cnt": 1,
            "traffic_cnt": 19,
            "peer_ip_nunique": 4,
            "peer_port_nunique": 3,
            "peer_segment_nunique": 2,
            "peer_ip_entropy": 1.2,
            "peer_port_entropy": 0.7,
            "peer_ip_top1_sessions_share": 0.5,
            "peer_port_top1_sessions_share": 0.6,
            "peer_ip_top1_bytes_share": 0.4,
            "peer_port_top1_bytes_share": 0.4,
            "max_sessions_per_minute": 8,
            "high_risk_port_sessions_cnt": 2,
            "admin_port_sessions_cnt": 1,
            "fileshare_port_sessions_cnt": 0,
            "directory_port_sessions_cnt": 0,
            "db_port_sessions_cnt": 0,
        },
    ]

    delta_df = spark.createDataFrame(data)

    delta_path = str(tmp_path / "delta")
    (
        delta_df.write
        .mode("overwrite")
        .parquet(delta_path)
    )

    # Configure FG-A builder to read from this path.
    config = FGABuilderConfig(
        project_name="ndr-unit-test",
        region_name="eu-west-1",
        delta_s3_prefix=delta_path,
        output_s3_prefix=str(tmp_path / "fg_a"),

        mini_batch_id="batch-1",
        feature_spec_version="fga_v1_test",
        feature_group_name_offline=None,
        feature_group_name_online=None,
        write_to_feature_store=False,
    )

    job = FGABuilderJob(spark=spark, config=config)

    # Monkey-patch _compute_anchor_ts to use our known anchor time.
    job._compute_anchor_ts = lambda df: anchor  # type: ignore

    job._run_impl()

    # Read back FG-A Parquet.
    fg_a_df = spark.read.parquet(config.output_s3_prefix)

    rows = fg_a_df.collect()
    assert rows, "FG-A output is empty"

    # We expect exactly one host and multiple windows; check 15m window metrics.
    row_15m = [r for r in rows if r.window_label == "w_15m"][0]

    assert row_15m.host_ip == "10.0.0.1"
    assert row_15m.sessions_cnt_w_15m == 20  # only last slice within 15m window
    assert row_15m.bytes_src_sum_w_15m == 2000.0
    assert row_15m.bytes_dst_sum_w_15m == 1000.0

    # Check that context fields exist.
    assert hasattr(row_15m, "hour_of_day")
    assert hasattr(row_15m, "is_working_hours")
    assert hasattr(row_15m, "is_weekend")
