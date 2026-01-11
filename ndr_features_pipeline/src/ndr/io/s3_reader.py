from pyspark.sql import SparkSession, DataFrame


class S3Reader:
    """Wrapper around SparkSession for reading S3 JSON Lines GZIP input."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read_jsonlines_gzip(
        self,
        prefix: str,
        projection: list[str] | None = None,
    ) -> DataFrame:
        """Read gzipped JSON Lines from an S3 prefix into a Spark DataFrame."""
        df = (
            self._spark.read
            .option("compression", "gzip")
            .json(prefix)
        )
        if projection:
            df = df.select(*projection)
        return df
