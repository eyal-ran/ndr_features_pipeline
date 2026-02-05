from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ndr.config.job_spec_loader import load_job_spec
from ndr.processing.base_runner import BaseProcessingJobRunner
from ndr.processing.inference_predictions_spec import (
    InferencePredictionsRuntimeConfig,
    InferenceSpec,
    parse_inference_spec,
)
from ndr.processing.output_paths import build_batch_output_prefix


logger = logging.getLogger(__name__)


@dataclass
class PredictionFeatureJoinRuntimeConfig:
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
    ) -> None:
        super().__init__(spark)
        self.runtime_config = runtime_config
        self.inference_spec = inference_spec

    def run(self) -> None:
        if not self.inference_spec.join_output:
            raise ValueError("Inference JobSpec does not include join_output configuration")

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
        prediction_columns = set(predictions_df.columns)
        feature_columns = [
            col
            for col in features_df.columns
            if col not in prediction_columns or col in join_keys
        ]
        return features_df.select(*feature_columns)

    def _write_joined(self, df: DataFrame) -> None:
        join_output = self.inference_spec.join_output
        if join_output is None:
            raise ValueError("join_output is required to write joined data")
        df = df.withColumn("dt", F.date_format(F.col("window_end_ts"), "yyyy-MM-dd"))
        output_prefix = build_batch_output_prefix(
            base_prefix=join_output.s3_prefix,
            dataset=join_output.dataset,
            batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
            batch_id=self.runtime_config.mini_batch_id,
        )
        partition_cols = join_output.partition_keys
        logger.info("Writing joined predictions to %s", output_prefix)
        (
            df.repartition(*partition_cols)
            .write.mode(join_output.write_mode)
            .partitionBy(*partition_cols)
            .parquet(output_prefix)
        )


def run_prediction_feature_join_from_runtime_config(
    runtime_config: PredictionFeatureJoinRuntimeConfig,
) -> None:
    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="inference_predictions",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    inference_spec = parse_inference_spec(job_spec)
    spark = SparkSession.builder.getOrCreate()
    job = PredictionFeatureJoinJob(spark, runtime_config, inference_spec)
    job.run()
