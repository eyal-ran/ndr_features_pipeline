from pyspark.sql import SparkSession
from ndr.processing import delta_builder_operators as ops


def _make_spark():
    return SparkSession.builder.master("local[1]").appName("test-delta-ops").getOrCreate()


def test_apply_base_counts_and_sums_smoke():
    spark = _make_spark()
    data = [
        {
            "host_ip": "10.0.0.1",
            "role": "outbound",
            "slice_start_ts": "2026-01-01T00:00:00Z",
            "slice_end_ts": "2026-01-01T00:15:00Z",
            "dt": "2026-01-01",
            "hh": "00",
            "mm": "00",
            "bytes_sent": 100.0,
            "bytes_recv": 200.0,
            "duration_sec": 10.0,
            "event_action": "allow",
            "event_reason": "tcp-rst-from-server",
            "event_name": "TRAFFIC",
            "network_transport": "TCP",
        }
    ]
    df = spark.createDataFrame(data)
    result = ops.apply_base_counts_and_sums(df, params={})
    row = result.collect()[0]
    assert row.sessions_cnt == 1
    assert row.bytes_sent_sum == 100.0
    spark.stop()
