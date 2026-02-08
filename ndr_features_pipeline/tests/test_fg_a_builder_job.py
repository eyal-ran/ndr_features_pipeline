
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
from ndr.processing.output_paths import build_batch_output_prefix


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
        delta_s3_prefix=delta_path,
        output_s3_prefix=str(tmp_path / "fg_a"),

        mini_batch_id="batch-1",
        batch_start_ts_iso=anchor.isoformat() + "Z",
        feature_spec_version="fga_v1_test",
    )

    job = FGABuilderJob(spark=spark, config=config)

    # Monkey-patch _compute_anchor_ts to use our known anchor time.
    job._compute_anchor_ts = lambda df: anchor  # type: ignore

    job._run_impl()

    # Read back FG-A Parquet.
    output_path = build_batch_output_prefix(
        base_prefix=config.output_s3_prefix,
        dataset="fg_a",
        batch_start_ts_iso=config.batch_start_ts_iso,
        batch_id=config.mini_batch_id,
    )
    fg_a_df = spark.read.parquet(output_path)

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


def test_fg_a_builder_novelty_and_high_risk_segments(tmp_path):
    spark = _create_spark()

    anchor = dt.datetime(2025, 1, 1, 12, 0, 0)
    slice_end = anchor

    delta_df = spark.createDataFrame(
        [
            {
                "mini_batch_id": "batch-1",
                "host_ip": "10.0.0.1",
                "role": "outbound",
                "slice_start_ts": slice_end - dt.timedelta(minutes=15),
                "slice_end_ts": slice_end,
                "sessions_cnt": 10,
                "allow_cnt": 10,
                "deny_cnt": 0,
                "alert_cnt": 0,
                "rst_cnt": 0,
                "aged_out_cnt": 0,
                "zero_reply_cnt": 0,
                "short_session_cnt": 0,
                "bytes_src_sum": 100.0,
                "bytes_dst_sum": 50.0,
                "duration_sum": 5.0,
                "tcp_cnt": 10,
                "udp_cnt": 0,
                "icmp_cnt": 0,
                "threat_cnt": 0,
                "traffic_cnt": 10,
                "peer_ip_nunique": 2,
                "peer_port_nunique": 2,
                "peer_segment_nunique": 1,
                "peer_ip_entropy": 1.0,
                "peer_port_entropy": 0.7,
                "peer_ip_top1_sessions_share": 0.5,
                "peer_port_top1_sessions_share": 0.5,
                "peer_ip_top1_bytes_share": 0.5,
                "peer_port_top1_bytes_share": 0.5,
                "max_sessions_per_minute": 5,
                "high_risk_port_sessions_cnt": 0,
                "admin_port_sessions_cnt": 0,
                "fileshare_port_sessions_cnt": 0,
                "directory_port_sessions_cnt": 0,
                "db_port_sessions_cnt": 0,
            }
        ]
    )

    pair_context_df = spark.createDataFrame(
        [
            {
                "host_ip": "10.0.0.1",
                "role": "outbound",
                "peer_ip": "1.1.1.1",
                "peer_port": 80,
                "slice_start_ts": slice_end - dt.timedelta(minutes=15),
                "slice_end_ts": slice_end,
                "sessions_cnt": 5,
                "event_ts": slice_end,
            },
            {
                "host_ip": "10.0.0.1",
                "role": "outbound",
                "peer_ip": "2.2.2.2",
                "peer_port": 443,
                "slice_start_ts": slice_end - dt.timedelta(minutes=15),
                "slice_end_ts": slice_end,
                "sessions_cnt": 5,
                "event_ts": slice_end,
            },
            {
                "host_ip": "10.0.0.1",
                "role": "outbound",
                "peer_ip": "192.168.1.10",
                "peer_port": 22,
                "slice_start_ts": slice_end - dt.timedelta(minutes=15),
                "slice_end_ts": slice_end,
                "sessions_cnt": 3,
                "event_ts": slice_end,
            },
        ]
    ).withColumn("pair_key", F.concat_ws("|", F.col("peer_ip"), F.col("peer_port")))

    lookback_df = spark.createDataFrame(
        [
            {
                "host_ip": "10.0.0.1",
                "peer_ip": "1.1.1.1",
                "peer_port": 80,
                "pair_key": "1.1.1.1|80",
                "peer_seen_count": 1,
                "port_seen_count": 1,
                "pair_seen_count": 1,
            }
        ]
    )

    delta_path = str(tmp_path / "delta")
    pair_context_path = str(tmp_path / "pair_context")
    lookback_path = str(tmp_path / "lookback")
    delta_df.write.mode("overwrite").parquet(delta_path)
    pair_context_df.write.mode("overwrite").parquet(pair_context_path)
    lookback_df.write.mode("overwrite").parquet(lookback_path)

    config = FGABuilderConfig(
        project_name="ndr-unit-test",
        delta_s3_prefix=delta_path,
        output_s3_prefix=str(tmp_path / "fg_a"),
        mini_batch_id="batch-1",
        batch_start_ts_iso=anchor.isoformat() + "Z",
        feature_spec_version="fga_v1_test",
        pair_context_s3_prefix=pair_context_path,
        lookback30d_s3_prefix=lookback_path,
        lookback30d_thresholds={"peer": 1, "port": 1, "pair": 1},
        high_risk_segments=["192.168.1.0/24"],
    )

    job = FGABuilderJob(spark=spark, config=config)
    job._compute_anchor_ts = lambda df: anchor  # type: ignore
    job._run_impl()

    output_path = build_batch_output_prefix(
        base_prefix=config.output_s3_prefix,
        dataset="fg_a",
        batch_start_ts_iso=config.batch_start_ts_iso,
        batch_id=config.mini_batch_id,
    )
    fg_a_df = spark.read.parquet(output_path)
    row_15m = [r for r in fg_a_df.collect() if r.window_label == "w_15m"][0]

    assert row_15m.new_peer_cnt_lookback30d_w_15m == 2
    assert row_15m.rare_peer_cnt_lookback30d_w_15m == 1
    assert row_15m.new_dst_port_cnt_lookback30d_w_15m == 2
    assert row_15m.new_pair_cnt_lookback30d_w_15m == 2
    assert row_15m.high_risk_segment_sessions_cnt_w_15m == 3
    assert row_15m.has_high_risk_segment_interaction_w_15m == 1
