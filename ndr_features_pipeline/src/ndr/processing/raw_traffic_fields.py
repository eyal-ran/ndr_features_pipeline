"""Utilities for specification-driven raw Palo Alto traffic field normalization."""

from __future__ import annotations

from pyspark.sql import DataFrame


REQUIRED_CANONICAL_TRAFFIC_FIELDS = (
    "source_ip",
    "destination_ip",
    "destination_port",
    "event_start",
    "event_end",
)

REQUIRED_DELTA_TRAFFIC_FIELDS = (
    "source_ip",
    "destination_ip",
    "source_port",
    "destination_port",
    "source_bytes",
    "destination_bytes",
    "event_start",
    "event_end",
)


def normalize_raw_traffic_fields(
    df: DataFrame,
    *,
    field_mapping: dict[str, str] | None,
    required_canonical_fields: tuple[str, ...],
    context_name: str,
) -> DataFrame:
    """Rename source columns to canonical names and validate required columns.

    Parameters
    ----------
    df:
        Input Spark DataFrame.
    field_mapping:
        Mapping of canonical name -> source column name.
    required_canonical_fields:
        Required columns expected after normalization.
    context_name:
        Name included in validation error messages.
    """
    mapping = field_mapping or {}
    source_columns = set(df.columns)

    for canonical_name, source_name in mapping.items():
        if source_name not in source_columns:
            raise ValueError(
                f"{context_name}: missing mapped source column '{source_name}' "
                f"for canonical field '{canonical_name}'"
            )

        if canonical_name == source_name:
            continue

        if canonical_name in source_columns and canonical_name != source_name:
            raise ValueError(
                f"{context_name}: cannot map '{source_name}' to '{canonical_name}' because "
                f"canonical column already exists in input"
            )

        df = df.withColumnRenamed(source_name, canonical_name)
        source_columns.discard(source_name)
        source_columns.add(canonical_name)

    missing = [name for name in required_canonical_fields if name not in source_columns]
    if missing:
        raise ValueError(f"{context_name}: missing required canonical columns: {missing}")

    return df
