"""Generate feature catalogs (Markdown + Parquet) for a feature spec."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import boto3
from pyspark.sql import SparkSession

from ndr.catalog.feature_catalog import build_feature_catalog


def _write_markdown(markdown: str, output_path: str) -> None:
    """Execute the write markdown stage of the workflow."""
    if output_path.startswith("s3://"):
        bucket, key = output_path[5:].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=markdown.encode("utf-8"))
        return
    Path(output_path).write_text(markdown, encoding="utf-8")


def _render_markdown(rows: List[dict]) -> str:
    """Execute the render markdown stage of the workflow."""
    headers = [
        "feature_id",
        "feature_number",
        "feature_name",
        "feature_group",
        "feature_formula",
        "feature_description",
        "creator_script",
        "upstream_dependencies",
        "feature_spec_version",
        "data_type",
        "nullable",
        "default_value",
        "downstream_consumers",
        "feature_group_name_offline",
        "feature_group_name_online",
        "s3_storage_path",
    ]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        values = [str(row.get(header, "")) for header in headers]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Build NDR feature catalogs.")
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--output-parquet-path", required=True)
    parser.add_argument("--output-markdown-path", required=True)
    parser.add_argument("--fg-a-offline-name")
    parser.add_argument("--fg-a-online-name")
    parser.add_argument("--fg-c-offline-name")
    parser.add_argument("--fg-c-online-name")
    parser.add_argument("--fg-a-s3-path")
    parser.add_argument("--fg-c-s3-path")
    args = parser.parse_args()

    bundle = build_feature_catalog(
        feature_spec_version=args.feature_spec_version,
        fg_a_offline_name=args.fg_a_offline_name,
        fg_a_online_name=args.fg_a_online_name,
        fg_c_offline_name=args.fg_c_offline_name,
        fg_c_online_name=args.fg_c_online_name,
        fg_a_s3_path=args.fg_a_s3_path,
        fg_c_s3_path=args.fg_c_s3_path,
    )

    rows = [entry.to_dict() for entry in bundle.model_inputs + bundle.non_model_fields]

    spark = (
        SparkSession.builder.appName("feature_catalog_builder")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").parquet(args.output_parquet_path)

    markdown = _render_markdown(rows)
    _write_markdown(markdown, args.output_markdown_path)

    spark.stop()


if __name__ == "__main__":
    main()

