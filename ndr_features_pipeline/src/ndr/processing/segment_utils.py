from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def add_segment_id(
    df: DataFrame,
    ip_col: str,
    segment_mapping: Dict[str, Any] | None = None,
) -> DataFrame:
    """Add a segment_id column based on an IP column.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.
    ip_col : str
        Column name containing the IP address to segment.
    segment_mapping : dict
        Optional mapping configuration with keys:
        - strategy: "ipv4_prefix" (default)
        - prefix_length: 24 (default)
    """
    seg_cfg = segment_mapping or {}
    strategy = seg_cfg.get("strategy", "ipv4_prefix")
    prefix_len = int(seg_cfg.get("prefix_length", 24))

    if strategy != "ipv4_prefix":
        strategy = "ipv4_prefix"

    def ipv4_prefix(ip: str) -> str:
        if ip is None:
            return "SEG_UNKNOWN"
        parts = ip.split(".")
        if len(parts) != 4:
            return "SEG_UNKNOWN"
        try:
            octets = [int(p) for p in parts]
        except ValueError:
            return "SEG_UNKNOWN"
        if prefix_len == 24:
            return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
        if prefix_len == 16:
            return f"{octets[0]}.{octets[1]}.0.0/16"
        return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"

    ipv4_prefix_udf = F.udf(ipv4_prefix, T.StringType())
    return df.withColumn("segment_id", ipv4_prefix_udf(F.col(ip_col)))
