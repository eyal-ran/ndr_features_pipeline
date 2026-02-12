"""NDR s3 writer module."""
from pyspark.sql import DataFrame



class S3Writer:
    """Writes Spark DataFrames to S3 with Parquet/partitioning semantics."""

    def write_parquet(
        self,
        df: DataFrame,
        base_path: str,
        partition_cols: list[str] | None = None,
        mode: str = "overwrite",
    ) -> None:
        """Write the DataFrame as Parquet to S3, optionally partitioned."""
        writer = df.write.mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(base_path)

    def write_parquet_partitioned(
        self,
        df: DataFrame,
        base_prefix: str,
        partition_cols: list[str],
        mode: str = "overwrite",
    ) -> None:
        """Write the DataFrame as partitioned Parquet to S3."""
        (
            df.write
            .mode(mode)
            .partitionBy(*partition_cols)
            .parquet(base_prefix)
        )
