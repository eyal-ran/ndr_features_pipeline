"""NDR delta builder operators module."""
from typing import Dict, Any


from pyspark.sql import DataFrame, functions as F


def apply_base_counts_and_sums(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """Compute base counts and sums per host/role/slice."""
    group_keys = ["host_ip", "role", "slice_start_ts", "slice_end_ts", "dt", "hh", "mm"]

    agg = (
        df.groupBy(*group_keys)
        .agg(
            F.count(F.lit(1)).alias("sessions_cnt"),
            F.sum(F.coalesce(F.col("bytes_sent"), F.lit(0))).alias("bytes_sent_sum"),
            F.sum(F.coalesce(F.col("bytes_recv"), F.lit(0))).alias("bytes_recv_sum"),
            F.sum(F.coalesce(F.col("duration_sec"), F.lit(0.0))).alias("duration_sum"),
            F.sum(F.when(F.col("event_action") == "allow", 1).otherwise(0)).alias("allow_cnt"),
            F.sum(F.when(F.col("event_action") == "drop", 1).otherwise(0)).alias("drop_cnt"),
            F.sum(F.when(F.col("event_action") == "alert", 1).otherwise(0)).alias("alert_cnt"),
            F.sum(F.when(F.col("event_reason").startswith("tcp-rst"), 1).otherwise(0)).alias("rst_cnt"),
            F.sum(F.when(F.col("event_reason") == "aged-out", 1).otherwise(0)).alias("aged_out_cnt"),
            F.sum(F.when(F.col("event_name") == "THREAT", 1).otherwise(0)).alias("threat_cnt"),
            F.sum(F.when(F.col("event_name") == "TRAFFIC", 1).otherwise(0)).alias("traffic_cnt"),
            F.sum(F.when(F.col("network_transport") == "TCP", 1).otherwise(0)).alias("tcp_cnt"),
            F.sum(F.when(F.col("network_transport") == "UDP", 1).otherwise(0)).alias("udp_cnt"),
            F.sum(F.when(F.col("network_transport") == "ICMP", 1).otherwise(0)).alias("icmp_cnt"),
        )
    )
    return agg


def apply_port_category_counters(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """Compute session counts for configured port categories."""
    port_sets: Dict[str, Any] = params.get("port_sets", {})
    group_keys = ["host_ip", "role", "slice_start_ts", "slice_end_ts", "dt", "hh", "mm"]
    agg_exprs = []
    for set_name, ports in port_sets.items():
        col_name = f"{set_name.lower()}_sessions_cnt"
        expr = F.sum(
            F.when(F.col("peer_port").isin([int(p) for p in ports]), 1).otherwise(0)
        ).alias(col_name)
        agg_exprs.append(expr)
    if not agg_exprs:
        return df.groupBy(*group_keys).agg(F.count(F.lit(1)).alias("sessions_cnt_dummy")).drop("sessions_cnt_dummy")
    return df.groupBy(*group_keys).agg(*agg_exprs)


def apply_burst_metrics(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """Compute max_sessions_per_minute per host/role/slice."""
    df_min = df.withColumn(
        "minute_bucket",
        F.date_trunc("minute", F.col("event_start")),
    )
    counts = (
        df_min.groupBy("host_ip", "role", "slice_start_ts", "slice_end_ts", "dt", "hh", "mm", "minute_bucket")
        .agg(F.count(F.lit(1)).alias("sessions_per_minute"))
    )
    burst = (
        counts.groupBy("host_ip", "role", "slice_start_ts", "slice_end_ts", "dt", "hh", "mm")
        .agg(F.max("sessions_per_minute").alias("max_sessions_per_minute"))
    )
    return burst


def apply_quantiles(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """Compute approximate quantiles for bytes and duration within a slice."""
    quantiles = params.get("quantiles", [0.25, 0.5, 0.75, 0.95, 0.99])
    rel_acc = params.get("relative_error", 0.01)
    group_keys = ["host_ip", "role", "slice_start_ts", "slice_end_ts", "dt", "hh", "mm"]

    agg = (
        df.groupBy(*group_keys)
        .agg(
            F.expr(f"percentile_approx(bytes_sent, array{quantiles}, {rel_acc})").alias("bytes_sent_quantiles"),
            F.expr(f"percentile_approx(bytes_recv, array{quantiles}, {rel_acc})").alias("bytes_recv_quantiles"),
            F.expr(f"percentile_approx(duration_sec, array{quantiles}, {rel_acc})").alias("duration_quantiles"),
        )
    )
    return agg
