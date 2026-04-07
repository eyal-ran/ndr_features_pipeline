"""NDR prediction feature join job module."""

from __future__ import annotations


import logging
import time
from dataclasses import dataclass
from typing import Any, List

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ndr.config.job_spec_loader import load_job_spec
from ndr.config.batch_index_loader import BatchIndexLoader
from ndr.processing.base_runner import BaseProcessingJobRunner
from ndr.processing.inference_predictions_spec import (
    InferencePredictionsRuntimeConfig,
    InferenceSpec,
    InferenceOutputSpec,
    parse_inference_spec,
)
from ndr.processing.output_paths import build_batch_output_prefix
from ndr.processing.prediction_feature_join_spec import (
    PredictionFeatureJoinDestinationRedshift,
    PredictionFeatureJoinSpec,
    parse_prediction_feature_join_spec,
)


logger = logging.getLogger(__name__)


def _validate_required_columns(columns: List[str], required: List[str], context: str) -> None:
    """Validate required publication columns are present before sink writes."""
    missing = sorted(set(required).difference(columns))
    if missing:
        raise ValueError(f"{context} missing required columns: {missing}")


def _validate_publication_output_contract(
    columns: List[str],
    join_keys: List[str],
    output_spec: InferenceOutputSpec,
    destination_type: str,
    score_column: str,
) -> None:
    """Validate publication output contract and sink-specific requirements."""
    required_publication_keys = join_keys + ["feature_spec_version", score_column, "dt"]
    _validate_required_columns(columns, required_publication_keys, "prediction_feature_join output")

    _validate_required_columns(columns, list(output_spec.partition_keys), "prediction_feature_join partition")
    if output_spec.write_mode.lower() != "overwrite":
        raise ValueError(
            "prediction_feature_join publication requires write_mode='overwrite' for idempotent retries/replays"
        )

    if destination_type == "redshift":
        redshift_required = join_keys + ["feature_spec_version", score_column]
        _validate_required_columns(columns, redshift_required, "prediction_feature_join redshift")


@dataclass
class PredictionFeatureJoinRuntimeConfig:
    """Data container for PredictionFeatureJoinRuntimeConfig."""
    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str
    ml_project_name: str
    batch_index_table_name: str | None = None


def _normalize_exact_batch_index_prefix(prefix: str) -> str:
    cleaned = prefix.rstrip("/")
    for suffix in ("/part-00000.parquet", "/publication_payload.json"):
        if cleaned.endswith(suffix):
            return cleaned[: -len(suffix)]
    return cleaned


class PredictionFeatureJoinJob(BaseProcessingJobRunner):
    """Join predictions with features for downstream ingestion."""

    def __init__(
        self,
        spark: SparkSession,
        runtime_config: PredictionFeatureJoinRuntimeConfig,
        inference_spec: InferenceSpec,
        join_spec: PredictionFeatureJoinSpec,
    ) -> None:
        """Initialize the instance with required clients and runtime configuration."""
        super().__init__(spark)
        self.runtime_config = runtime_config
        self.inference_spec = inference_spec
        self.join_spec = join_spec

    def run(self) -> None:
        """Execute the full workflow for this job runner."""
        predictions_df = self._load_predictions()
        if predictions_df.rdd.isEmpty():
            logger.warning("Predictions are empty; skipping join")
            return

        features_df = self._load_features()
        if features_df.rdd.isEmpty():
            logger.warning("Features are empty; skipping join")
            return

        join_keys = self.inference_spec.join_keys + ["feature_spec_version"]
        deduped_features = self._dedupe_feature_columns(features_df, predictions_df, join_keys)
        joined = predictions_df.join(deduped_features, on=join_keys, how="inner")
        self._write_joined(joined)

    def _load_predictions(self) -> DataFrame:
        """Execute the load predictions stage of the workflow."""
        output_spec = self.inference_spec.output
        if output_spec.exact_prefix:
            predictions_prefix = _normalize_exact_batch_index_prefix(output_spec.s3_prefix)
        else:
            predictions_prefix = build_batch_output_prefix(
                base_prefix=output_spec.s3_prefix,
                dataset=output_spec.dataset,
                batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
                batch_id=self.runtime_config.mini_batch_id,
            )
        logger.info("Reading predictions from %s", predictions_prefix)
        df = self.spark.read.option("mergeSchema", "true").parquet(predictions_prefix)
        if "feature_spec_version" in df.columns:
            df = df.filter(F.col("feature_spec_version") == self.runtime_config.feature_spec_version)
        return df

    def _load_features(self) -> DataFrame:
        """Execute the load features stage of the workflow."""
        runtime = InferencePredictionsRuntimeConfig(
            project_name=self.runtime_config.project_name,
            feature_spec_version=self.runtime_config.feature_spec_version,
            mini_batch_id=self.runtime_config.mini_batch_id,
            batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
            batch_end_ts_iso=self.runtime_config.batch_end_ts_iso,
            ml_project_name=self.runtime_config.ml_project_name,
        )
        from ndr.processing.inference_predictions_job import InferencePredictionsJob

        inference_job = InferencePredictionsJob(self.spark, runtime, self.inference_spec)
        return inference_job.load_features()

    def _dedupe_feature_columns(
        self, features_df: DataFrame, predictions_df: DataFrame, join_keys: List[str]
    ) -> DataFrame:
        """Execute the dedupe feature columns stage of the workflow."""
        prediction_columns = set(predictions_df.columns)
        feature_columns = [
            col
            for col in features_df.columns
            if col not in prediction_columns or col in join_keys
        ]
        return features_df.select(*feature_columns)

    def _write_joined(self, df: DataFrame) -> None:
        """Execute the write joined stage of the workflow."""
        df = df.withColumn("dt", F.date_format(F.col("window_end_ts"), "yyyy-MM-dd"))
        if self.join_spec.destination_type == "s3":
            if not self.join_spec.s3_output:
                raise ValueError("prediction_feature_join requires s3_output for S3 destination")
            _validate_publication_output_contract(
                df.columns,
                self.inference_spec.join_keys,
                self.join_spec.s3_output,
                "s3",
                self.inference_spec.prediction_schema.score_column,
            )
            self._write_joined_to_s3(df, self.join_spec.s3_output)
            return

        if self.join_spec.destination_type == "redshift":
            if not self.join_spec.s3_output or not self.join_spec.redshift_output:
                raise ValueError("prediction_feature_join requires staging s3_output and redshift_output")
            _validate_publication_output_contract(
                df.columns,
                self.inference_spec.join_keys,
                self.join_spec.s3_output,
                "redshift",
                self.inference_spec.prediction_schema.score_column,
            )
            output_prefix = self._write_joined_to_s3(df, self.join_spec.s3_output)
            self._copy_to_redshift(output_prefix, self.join_spec.redshift_output)
            return

        raise ValueError(f"Unsupported prediction_feature_join destination {self.join_spec.destination_type}")

    def _write_joined_to_s3(self, df: DataFrame, output_spec: InferenceOutputSpec) -> str:
        """Execute the write joined to s3 stage of the workflow."""
        if output_spec.format.lower() != "parquet":
            raise ValueError("prediction_feature_join currently supports parquet output only")
        if output_spec.exact_prefix:
            output_prefix = _normalize_exact_batch_index_prefix(output_spec.s3_prefix)
        else:
            output_prefix = build_batch_output_prefix(
                base_prefix=output_spec.s3_prefix,
                dataset=output_spec.dataset,
                batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
                batch_id=self.runtime_config.mini_batch_id,
            )
        partition_cols = output_spec.partition_keys
        logger.info("Writing joined predictions to %s", output_prefix)
        (
            df.repartition(*partition_cols)
            .write.mode(output_spec.write_mode)
            .partitionBy(*partition_cols)
            .parquet(output_prefix)
        )
        return output_prefix

    def _copy_to_redshift(
        self,
        s3_prefix: str,
        redshift_spec: PredictionFeatureJoinDestinationRedshift,
    ) -> None:
        """Execute the copy to redshift stage of the workflow."""
        if not s3_prefix.startswith("s3://"):
            raise ValueError("prediction_feature_join redshift staging prefix must be an s3:// URI")
        data_api = boto3.client("redshift-data", region_name=redshift_spec.region)
        for sql in redshift_spec.pre_sql:
            self._execute_statement(data_api, redshift_spec, sql)

        copy_sql = (
            "COPY "
            f"{redshift_spec.schema}.{redshift_spec.table} "
            f"FROM '{s3_prefix}' "
            f"IAM_ROLE '{redshift_spec.iam_role}' "
            "FORMAT AS PARQUET"
        )
        if redshift_spec.copy_options:
            copy_sql = f"{copy_sql} {redshift_spec.copy_options}"
        self._execute_statement(data_api, redshift_spec, copy_sql)

        for sql in redshift_spec.post_sql:
            self._execute_statement(data_api, redshift_spec, sql)

    def _execute_statement(
        self,
        data_api: Any,
        redshift_spec: PredictionFeatureJoinDestinationRedshift,
        sql: str,
    ) -> None:
        """Execute the execute statement stage of the workflow."""
        params = dict(
            ClusterIdentifier=redshift_spec.cluster_identifier,
            Database=redshift_spec.database,
            SecretArn=redshift_spec.secret_arn,
            Sql=sql,
        )
        if redshift_spec.db_user:
            params["DbUser"] = redshift_spec.db_user
        response = data_api.execute_statement(**params)
        statement_id = response["Id"]
        self._wait_for_statement(data_api, statement_id)

    def _wait_for_statement(self, data_api: Any, statement_id: str) -> None:
        """Execute the wait for statement stage of the workflow."""
        while True:
            response = data_api.describe_statement(Id=statement_id)
            status = response["Status"]
            if status == "FINISHED":
                return
            if status in {"FAILED", "ABORTED"}:
                raise RuntimeError(f"Redshift Data API statement {statement_id} failed: {response}")
            time.sleep(2)


def run_prediction_feature_join_from_runtime_config(
    runtime_config: PredictionFeatureJoinRuntimeConfig,
) -> None:
    """Execute the run prediction feature join from runtime config stage of the workflow."""
    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="inference_predictions",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    inference_spec = parse_inference_spec(job_spec)
    join_job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="prediction_feature_join",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    join_spec = parse_prediction_feature_join_spec(join_job_spec)
    record = BatchIndexLoader(table_name=runtime_config.batch_index_table_name).get_batch(
        project_name=runtime_config.project_name,
        batch_id=runtime_config.mini_batch_id,
    )
    if record is not None:
        dpp = record.s3_prefixes.get("dpp", {})
        mlp_branch = record.s3_prefixes.get("mlp", {}).get(runtime_config.ml_project_name, {})
        fg_a = dpp.get("fg_a")
        fg_c = dpp.get("fg_c")
        if fg_a and "fg_a" in inference_spec.feature_inputs:
            inference_spec.feature_inputs["fg_a"].s3_prefix = str(fg_a)
            inference_spec.feature_inputs["fg_a"].exact_prefix = True
        if fg_c and "fg_c" in inference_spec.feature_inputs:
            inference_spec.feature_inputs["fg_c"].s3_prefix = str(fg_c)
            inference_spec.feature_inputs["fg_c"].exact_prefix = True
        predictions_prefix = mlp_branch.get("predictions")
        if predictions_prefix:
            inference_spec.output.s3_prefix = str(predictions_prefix)
            inference_spec.output.exact_prefix = True
        join_prefix = mlp_branch.get("prediction_join")
        if join_prefix and join_spec.s3_output is not None:
            join_spec.s3_output.s3_prefix = str(join_prefix)
            join_spec.s3_output.exact_prefix = True
    spark = SparkSession.builder.getOrCreate()
    job = PredictionFeatureJoinJob(spark, runtime_config, inference_spec, join_spec)
    job.run()
