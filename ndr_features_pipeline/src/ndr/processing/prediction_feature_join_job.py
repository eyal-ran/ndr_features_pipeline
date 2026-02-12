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


@dataclass
class PredictionFeatureJoinRuntimeConfig:
    """Data container for PredictionFeatureJoinRuntimeConfig."""
    project_name: str
    feature_spec_version: str
    mini_batch_id: str
    batch_start_ts_iso: str
    batch_end_ts_iso: str


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
            self._write_joined_to_s3(df, self.join_spec.s3_output)
            return

        if self.join_spec.destination_type == "redshift":
            if not self.join_spec.s3_output or not self.join_spec.redshift_output:
                raise ValueError("prediction_feature_join requires staging s3_output and redshift_output")
            output_prefix = self._write_joined_to_s3(df, self.join_spec.s3_output)
            self._copy_to_redshift(output_prefix, self.join_spec.redshift_output)
            return

        raise ValueError(f"Unsupported prediction_feature_join destination {self.join_spec.destination_type}")

    def _write_joined_to_s3(self, df: DataFrame, output_spec: InferenceOutputSpec) -> str:
        """Execute the write joined to s3 stage of the workflow."""
        if output_spec.format.lower() != "parquet":
            raise ValueError("prediction_feature_join currently supports parquet output only")
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
    spark = SparkSession.builder.getOrCreate()
    job = PredictionFeatureJoinJob(spark, runtime_config, inference_spec, join_spec)
    job.run()
