import sys
import types
from unittest.mock import MagicMock

# Spark stubs for import-time compatibility
pyspark_module = types.ModuleType("pyspark")
sql_module = types.ModuleType("pyspark.sql")

class _DataFrame:
    pass

class _SparkSession:
    pass

sql_module.DataFrame = _DataFrame
sql_module.SparkSession = _SparkSession
pyspark_module.sql = sql_module
sys.modules.setdefault("pyspark", pyspark_module)
sys.modules.setdefault("pyspark.sql", sql_module)

from ndr.processing.raw_traffic_fields import normalize_raw_traffic_fields


def test_normalize_raw_traffic_fields_renames_when_mapping_present():
    df = MagicMock()
    df.columns = ["src", "dst", "dport", "start", "end"]

    df1 = MagicMock(); df1.columns = ["source_ip", "dst", "dport", "start", "end"]
    df2 = MagicMock(); df2.columns = ["source_ip", "destination_ip", "dport", "start", "end"]
    df3 = MagicMock(); df3.columns = ["source_ip", "destination_ip", "destination_port", "start", "end"]
    df4 = MagicMock(); df4.columns = ["source_ip", "destination_ip", "destination_port", "event_start", "end"]
    df5 = MagicMock(); df5.columns = ["source_ip", "destination_ip", "destination_port", "event_start", "event_end"]

    df.withColumnRenamed.return_value = df1
    df1.withColumnRenamed.return_value = df2
    df2.withColumnRenamed.return_value = df3
    df3.withColumnRenamed.return_value = df4
    df4.withColumnRenamed.return_value = df5

    out = normalize_raw_traffic_fields(
        df,
        field_mapping={
            "source_ip": "src",
            "destination_ip": "dst",
            "destination_port": "dport",
            "event_start": "start",
            "event_end": "end",
        },
        required_canonical_fields=("source_ip", "destination_ip", "destination_port", "event_start", "event_end"),
        context_name="pair",
    )
    assert out is df5


def test_normalize_raw_traffic_fields_errors_on_missing_required():
    df = MagicMock()
    df.columns = ["source_ip", "destination_ip"]
    try:
        normalize_raw_traffic_fields(
            df,
            field_mapping={},
            required_canonical_fields=("source_ip", "destination_ip", "destination_port"),
            context_name="pair",
        )
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "missing required canonical columns" in str(exc)
