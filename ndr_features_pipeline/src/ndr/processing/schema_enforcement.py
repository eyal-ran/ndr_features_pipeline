"""Schema enforcement helpers for Spark DataFrames."""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T

from ndr.catalog.schema_manifest import SchemaManifest, SchemaField


TYPE_MAPPING: Dict[str, T.DataType] = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "timestamp": T.TimestampType(),
    "date": T.DateType(),
}


def _spark_type(data_type: str) -> T.DataType:
    """Execute the spark type stage of the workflow."""
    if data_type not in TYPE_MAPPING:
        raise ValueError(f"Unsupported schema data type: {data_type}")
    return TYPE_MAPPING[data_type]


def enforce_schema(
    df: DataFrame,
    manifest: SchemaManifest,
    dataset_name: str,
    logger,
) -> DataFrame:
    """Execute the enforce schema stage of the workflow."""
    missing_cols = [field.name for field in manifest.fields if field.name not in df.columns]
    if missing_cols:
        raise ValueError(
            f"{dataset_name} output missing required columns: {sorted(missing_cols)}"
        )

    updated = df
    for field in manifest.fields:
        updated = _coerce_field(updated, field, logger, dataset_name)
    return updated


def _coerce_field(
    df: DataFrame,
    field: SchemaField,
    logger,
    dataset_name: str,
) -> DataFrame:
    """Execute the coerce field stage of the workflow."""
    expected_type = _spark_type(field.data_type)
    actual_type = df.schema[field.name].dataType
    updated = df
    if type(actual_type) is not type(expected_type):
        logger.info(
            "Casting %s.%s from %s to %s.",
            dataset_name,
            field.name,
            actual_type,
            expected_type,
        )
        updated = updated.withColumn(field.name, F.col(field.name).cast(expected_type))

    if field.default_value is not None:
        updated = updated.withColumn(
            field.name,
            F.coalesce(F.col(field.name), F.lit(field.default_value).cast(expected_type)),
        )

    return updated

