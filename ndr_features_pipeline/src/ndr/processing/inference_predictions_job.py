from __future__ import annotations

import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Sequence, Tuple, TYPE_CHECKING

from ndr.processing.base_runner import BaseProcessingJobRunner
from ndr.processing.inference_predictions_spec import (
    InferenceModelSpec,
    InferencePredictionsRuntimeConfig,
    InferenceSpec,
    PredictionSchemaSpec,
    parse_inference_spec,
)
from ndr.processing.output_paths import build_batch_output_prefix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession
    from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


class SagemakerEndpointInvoker:
    """Invoke a SageMaker endpoint with retry and payload chunking."""

    def __init__(self, model_spec: InferenceModelSpec) -> None:
        self.model_spec = model_spec
        import boto3

        self.client = boto3.client("sagemaker-runtime", region_name=model_spec.region)

    def _payload_size_bytes(self, records: Sequence[Dict[str, Any]]) -> int:
        payload = json.dumps({"instances": records}).encode("utf-8")
        return len(payload)

    def _chunk_records(
        self, records: Sequence[Tuple[Dict[str, Any], Dict[str, Any]]]
    ) -> Iterable[List[Tuple[Dict[str, Any], Dict[str, Any]]]]:
        max_payload_bytes = int(self.model_spec.max_payload_mb * 1024 * 1024)
        current: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for record in records:
            if not current:
                current.append(record)
                continue
            candidate = current + [record]
            candidate_payload = [item[1] for item in candidate]
            if (
                len(candidate) > self.model_spec.max_records_per_payload
                or self._payload_size_bytes(candidate_payload) > max_payload_bytes
            ):
                yield current
                current = [record]
            else:
                current = candidate
        if current:
            yield current

    def _invoke_payload(self, payload: str) -> Tuple[List[Any], Dict[str, Any]]:
        for attempt in range(self.model_spec.max_retries + 1):
            try:
                response = self.client.invoke_endpoint(
                    EndpointName=self.model_spec.endpoint_name,
                    ContentType=self.model_spec.content_type,
                    Accept=self.model_spec.accept,
                    Body=payload,
                )
                raw = response["Body"].read()
                decoded = raw.decode("utf-8")
                data = json.loads(decoded)
                predictions = self._parse_predictions(data)
                return predictions, response
            except Exception as exc:  # pylint: disable=broad-except
                if attempt >= self.model_spec.max_retries:
                    raise
                sleep_seconds = self.model_spec.backoff_seconds * (2**attempt)
                logger.warning("Endpoint invoke failed (%s); retrying in %.1fs", exc, sleep_seconds)
                time.sleep(sleep_seconds)
        raise RuntimeError("Exceeded maximum retries for endpoint invocation")

    def _parse_predictions(self, payload: Any) -> List[Any]:
        if isinstance(payload, dict):
            if "predictions" in payload:
                return payload["predictions"]
            if "outputs" in payload:
                return payload["outputs"]
        if isinstance(payload, list):
            return payload
        raise ValueError("Unsupported prediction payload structure")

    def invoke_records(
        self,
        records: Sequence[Tuple[Dict[str, Any], Dict[str, Any]]],
        schema: PredictionSchemaSpec,
        runtime: InferencePredictionsRuntimeConfig,
    ) -> Iterable[Dict[str, Any]]:
        if not records:
            return []

        results: List[Dict[str, Any]] = []
        batches = list(self._chunk_records(records))
        inference_ts = datetime.now(timezone.utc)

        if self.model_spec.max_workers > 1 and len(batches) > 1:
            with ThreadPoolExecutor(max_workers=self.model_spec.max_workers) as executor:
                future_map = {
                    executor.submit(self._invoke_payload, self._build_payload(batch)): batch
                    for batch in batches
                }
                for future in as_completed(future_map):
                    batch = future_map[future]
                    predictions, response = future.result()
                    results.extend(
                        self._merge_predictions(batch, predictions, schema, runtime, inference_ts, response)
                    )
        else:
            for batch in batches:
                payload = self._build_payload(batch)
                predictions, response = self._invoke_payload(payload)
                results.extend(
                    self._merge_predictions(batch, predictions, schema, runtime, inference_ts, response)
                )

        return results

    def _build_payload(self, batch: Sequence[Tuple[Dict[str, Any], Dict[str, Any]]]) -> str:
        records = [item[1] for item in batch]
        return json.dumps({"instances": records})

    def _merge_predictions(
        self,
        batch: Sequence[Tuple[Dict[str, Any], Dict[str, Any]]],
        predictions: Sequence[Any],
        schema: PredictionSchemaSpec,
        runtime: InferencePredictionsRuntimeConfig,
        inference_ts: datetime,
        response: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if len(predictions) != len(batch):
            raise ValueError(
                "Prediction count does not match record count "
                f"({len(predictions)} vs {len(batch)})"
            )

        model_version = self.model_spec.model_version or response.get("ModelVersion")
        outputs: List[Dict[str, Any]] = []
        for (meta, _), prediction in zip(batch, predictions):
            score, label = self._extract_prediction(prediction, schema)
            record = {
                **meta,
                "mini_batch_id": runtime.mini_batch_id,
                schema.score_column: score,
                "model_version": model_version,
                "model_name": self.model_spec.model_name,
                "inference_ts": inference_ts,
                "record_id": str(uuid.uuid4()),
            }
            if schema.label_column:
                record[schema.label_column] = label
            outputs.append(record)
        return outputs

    def _extract_prediction(
        self, prediction: Any, schema: PredictionSchemaSpec
    ) -> Tuple[float | None, Any | None]:
        if isinstance(prediction, dict):
            score = (
                prediction.get(schema.score_column)
                or prediction.get("score")
                or prediction.get("probability")
                or prediction.get("prediction")
            )
            label = None
            if schema.label_column:
                label = prediction.get(schema.label_column) or prediction.get("label")
            return score, label
        if isinstance(prediction, (list, tuple)):
            if len(prediction) >= 2:
                return prediction[0], prediction[1]
            if len(prediction) == 1:
                return prediction[0], None
        if isinstance(prediction, (float, int)):
            return float(prediction), None
        return None, None


class InferencePredictionsJob(BaseProcessingJobRunner):
    """Spark processing job for decoupled inference predictions."""

    def __init__(
        self,
        spark: SparkSession,
        runtime_config: InferencePredictionsRuntimeConfig,
        inference_spec: InferenceSpec,
    ) -> None:
        super().__init__(spark)
        self.runtime_config = runtime_config
        self.inference_spec = inference_spec

    def run(self) -> None:
        logger.info(
            "Starting inference predictions job for project=%s mini_batch_id=%s",
            self.runtime_config.project_name,
            self.runtime_config.mini_batch_id,
        )
        features_df = self.load_features()
        if features_df.rdd.isEmpty():
            logger.warning("Feature DataFrame is empty; skipping inference")
            return
        predictions_df = self.invoke_model(features_df)
        if predictions_df.rdd.isEmpty():
            logger.warning("Prediction DataFrame is empty; nothing to write")
            return
        self.write_predictions(predictions_df)
        logger.info("Inference predictions job completed")

    def load_features(self) -> DataFrame:
        from pyspark.sql import functions as F

        join_keys = self.inference_spec.join_keys
        input_specs = self.inference_spec.feature_inputs
        dataframes: List[DataFrame] = []

        for name, spec in input_specs.items():
            dataset = spec.dataset or name
            input_prefix = build_batch_output_prefix(
                base_prefix=spec.s3_prefix,
                dataset=dataset,
                batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
                batch_id=self.runtime_config.mini_batch_id,
            )
            logger.info("Reading features %s from %s", name, input_prefix)
            df = (
                self.spark.read
                .option("mergeSchema", "true")
                .parquet(input_prefix)
            )
            if "feature_spec_version" in df.columns:
                df = df.filter(F.col("feature_spec_version") == self.runtime_config.feature_spec_version)
            if df.rdd.isEmpty():
                message = f"Input {name} is empty at {input_prefix}"
                if spec.required:
                    raise ValueError(message)
                logger.warning(message)
                continue
            missing = [key for key in join_keys if key not in df.columns]
            if missing:
                raise ValueError(f"Input {name} missing join keys: {missing}")
            dataframes.append(df)

        if not dataframes:
            return self.spark.createDataFrame([], schema=StructType([]))

        combined = dataframes[0]
        for df in dataframes[1:]:
            combined = combined.join(df, on=join_keys, how="inner")

        if "feature_spec_version" not in combined.columns:
            combined = combined.withColumn("feature_spec_version", F.lit(self.runtime_config.feature_spec_version))

        return combined

    def invoke_model(self, df: DataFrame) -> DataFrame:
        join_keys = self.inference_spec.join_keys
        feature_columns = self._select_feature_columns(df)
        required_columns = set(join_keys + ["feature_spec_version"] + feature_columns)
        missing = required_columns.difference(df.columns)
        if missing:
            raise ValueError(f"Missing required columns for inference: {sorted(missing)}")

        input_df = df.select(*(join_keys + ["feature_spec_version"] + feature_columns))
        schema = self._build_prediction_schema(input_df)
        runtime = self.runtime_config
        model_spec = self.inference_spec.model
        prediction_schema = self.inference_spec.prediction_schema

        def _invoke_partition(rows: Iterable[Row]) -> Iterable[Dict[str, Any]]:
            invoker = SagemakerEndpointInvoker(model_spec)
            buffer: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
            outputs: List[Dict[str, Any]] = []

            def flush() -> None:
                nonlocal outputs
                if not buffer:
                    return
                outputs = invoker.invoke_records(buffer, prediction_schema, runtime)
                buffer.clear()

            for row in rows:
                row_dict = row.asDict()
                meta = {key: row_dict[key] for key in join_keys}
                meta["feature_spec_version"] = row_dict.get("feature_spec_version", runtime.feature_spec_version)
                features = {col: row_dict.get(col) for col in feature_columns}
                buffer.append((meta, features))
                if len(buffer) >= model_spec.max_records_per_payload:
                    flush()
                    for item in outputs:
                        yield item
            flush()
            for item in outputs:
                yield item

        rdd = input_df.rdd.mapPartitions(_invoke_partition)
        return self.spark.createDataFrame(rdd, schema=schema)

    def write_predictions(self, df: DataFrame) -> None:
        from pyspark.sql import functions as F

        output_spec = self.inference_spec.output
        if df.rdd.isEmpty():
            logger.warning("Prediction DataFrame is empty; nothing to write")
            return

        df = df.withColumn("dt", F.date_format(F.col("window_end_ts"), "yyyy-MM-dd"))
        if "model_version" not in df.columns:
            df = df.withColumn("model_version", F.lit(self.inference_spec.model.model_version))

        output_prefix = build_batch_output_prefix(
            base_prefix=output_spec.s3_prefix,
            dataset=output_spec.dataset,
            batch_start_ts_iso=self.runtime_config.batch_start_ts_iso,
            batch_id=self.runtime_config.mini_batch_id,
        )

        partition_cols = output_spec.partition_keys
        logger.info("Writing predictions to %s partitioned by %s", output_prefix, partition_cols)
        (
            df.repartition(*partition_cols)
            .write.mode(output_spec.write_mode)
            .partitionBy(*partition_cols)
            .parquet(output_prefix)
        )

    def _select_feature_columns(self, df: DataFrame) -> List[str]:
        if self.inference_spec.payload.feature_columns:
            missing = [
                col for col in self.inference_spec.payload.feature_columns if col not in df.columns
            ]
            if missing:
                raise ValueError(f"Feature columns missing from input DataFrame: {missing}")
            return list(self.inference_spec.payload.feature_columns)

        excluded = set(self.inference_spec.join_keys + ["feature_spec_version", "dt", "mini_batch_id"])
        return [col for col in df.columns if col not in excluded]

    def _build_prediction_schema(self, df: DataFrame) -> StructType:
        from pyspark.sql.types import (
            DoubleType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        join_key_fields = [StructField(key, df.schema[key].dataType, True) for key in self.inference_spec.join_keys]
        fields = join_key_fields + [
            StructField("feature_spec_version", StringType(), True),
            StructField("mini_batch_id", StringType(), True),
            StructField(self.inference_spec.prediction_schema.score_column, DoubleType(), True),
            StructField("model_version", StringType(), True),
            StructField("model_name", StringType(), True),
            StructField("inference_ts", TimestampType(), True),
            StructField("record_id", StringType(), True),
        ]
        if self.inference_spec.prediction_schema.label_column:
            fields.append(StructField(self.inference_spec.prediction_schema.label_column, StringType(), True))
        return StructType(fields)


def run_inference_predictions_from_runtime_config(
    runtime_config: InferencePredictionsRuntimeConfig,
) -> None:
    from ndr.config.job_spec_loader import load_job_spec
    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="inference_predictions",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    inference_spec = parse_inference_spec(job_spec)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    job = InferencePredictionsJob(spark, runtime_config, inference_spec)
    job.run()
