from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import tarfile
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from ndr.catalog.feature_catalog import build_fg_c_metric_names
from ndr.catalog.schema_manifest import build_fg_a_manifest, build_fg_c_manifest
from ndr.config.job_spec_loader import DDB_TABLE_ENV_VAR, LEGACY_DDB_TABLE_ENV_VAR
from ndr.processing.base_runner import BaseProcessingJobRunner
from ndr.processing.if_training_preprocessing import split_metadata_and_feature_columns
from ndr.processing.if_training_spec import IFTrainingRuntimeConfig, IFTrainingSpec, parse_if_training_spec
from ndr.processing.schema_enforcement import enforce_schema

logger = logging.getLogger(__name__)


class IFTrainingJob(BaseProcessingJobRunner):
    def __init__(
        self,
        spark,
        runtime_config: IFTrainingRuntimeConfig,
        training_spec: IFTrainingSpec,
        resolved_spec_payload: Dict[str, Any] | None = None,
    ) -> str:
        super().__init__(spark)
        self.runtime_config = runtime_config
        self.training_spec = training_spec
        self.resolved_spec_payload = resolved_spec_payload or {}

    def run(self) -> None:
        train_start, train_end = self._resolve_training_window()
        stage = "read_inputs"
        try:
            fg_a_df = self._read_windowed_input("fg_a", train_start, train_end)
            fg_c_df = self._read_windowed_input("fg_c", train_start, train_end)

            stage = "schema_enforcement"
            fg_a_df = enforce_schema(fg_a_df, build_fg_a_manifest(), "fg_a", logger)
            fg_c_df = enforce_schema(fg_c_df, build_fg_c_manifest(metrics=build_fg_c_metric_names()), "fg_c", logger)

            stage = "preflight"
            preflight = self._preflight_validation(fg_a_df, fg_c_df, train_start, train_end)
            stage = "join"
            joined_df = fg_a_df.join(fg_c_df, on=self.training_spec.join_keys, how="inner")

            metadata_columns, feature_columns = split_metadata_and_feature_columns(
                joined_df.columns,
                self.training_spec.join_keys,
            )
            feature_columns = [
                c for c in feature_columns
                if c not in {"record_id", "mini_batch_id", "window_start_ts", "hour_of_day", "day_of_week"}
            ]
            if not feature_columns:
                raise ValueError("No numeric feature columns available for IF training")

            split_ts = train_end - timedelta(days=30)
            train_df = joined_df.filter(joined_df.window_end_ts < split_ts)
            val_df = joined_df.filter(joined_df.window_end_ts >= split_ts)
            if train_df.rdd.isEmpty() or val_df.rdd.isEmpty():
                raise ValueError("Training or validation split is empty; unable to tune/train model")

            stage = "preprocessing_tune"
            tune_scaler, tune_outlier = self._fit_preprocessing_params(train_df, feature_columns)
            train_processed = self._apply_preprocessing_with_params(train_df, feature_columns, tune_scaler, tune_outlier)
            val_processed = self._apply_preprocessing_with_params(val_df, feature_columns, tune_scaler, tune_outlier)

            stage = "feature_selection"
            selected_features, selection_meta = self._select_features(
                train_processed=train_processed,
                val_processed=val_processed,
                feature_columns=feature_columns,
            )
            if not selected_features:
                raise ValueError("Feature selection produced an empty feature mask")

            stage = "tuning"
            tuning_summary, best_params, gates, val_metrics = self._tune_and_validate(
                train_processed=train_processed,
                val_processed=val_processed,
                selected_features=selected_features,
            )

            stage = "final_refit"
            final_scaler, final_outlier = self._fit_preprocessing_params(joined_df, feature_columns)
            full_processed = self._apply_preprocessing_with_params(joined_df, feature_columns, final_scaler, final_outlier)
            final_model = self._fit_final_model(full_processed, selected_features, best_params)

            metrics = self._build_metrics(
                full_processed=full_processed,
                train_processed=train_processed,
                val_processed=val_processed,
                all_features=feature_columns,
                selected_features=selected_features,
                tuning_summary=tuning_summary,
                gates=gates,
                val_metrics=val_metrics,
            )

            stage = "persist_artifacts"
            artifact_ctx = self._persist_artifacts(
                scaler_params=final_scaler,
                outlier_params=final_outlier,
                feature_mask=selected_features,
                metrics=metrics,
                tuning_summary=tuning_summary,
                model=final_model,
                train_start=train_start,
                train_end=train_end,
            )

            stage = "deploy"
            deployment_status = self._maybe_deploy(
                model_data_url=artifact_ctx["model_tar_s3_uri"],
                gates=gates,
            )

            latest_model_path = None
            if all(g["passed"] for g in gates.values()):
                latest_model_path = self._promote_latest_model_pointer(artifact_ctx)

            stage = "final_report"
            report_s3_uri = self._write_final_report_and_success(
                train_start=train_start,
                train_end=train_end,
                preflight=preflight,
                metadata_columns=metadata_columns,
                selected_features=selected_features,
                scaler_params=final_scaler,
                outlier_params=final_outlier,
                tuning_summary=tuning_summary,
                metrics=metrics,
                gates=gates,
                deployment_status=deployment_status,
                selection_meta=selection_meta,
                artifact_ctx=artifact_ctx,
                latest_model_path=latest_model_path,
            )

            stage = "experiment_tracking"
            self._log_sagemaker_experiments(
                metrics=metrics,
                tuning_summary=tuning_summary,
                deployment_status=deployment_status,
                preflight=preflight,
                report_s3_uri=report_s3_uri,
                selected_feature_count=len(selected_features),
                all_feature_count=len(feature_columns),
            )
        except Exception as exc:  # pylint: disable=broad-except
            self._write_failure_report(
                stage=stage,
                error=str(exc),
                train_start=train_start,
                train_end=train_end,
            )
            self._write_failure_experiment_artifact(
                stage=stage,
                error=str(exc),
                train_start=train_start,
                train_end=train_end,
            )
            raise

    def _resolve_training_window(self) -> Tuple[datetime, datetime]:
        exec_ts = self.runtime_config.execution_ts_iso.replace("Z", "+00:00")
        t0 = datetime.fromisoformat(exec_ts).astimezone(timezone.utc)
        month = t0.month - self.training_spec.window.gap_months
        year = t0.year
        while month <= 0:
            month += 12
            year -= 1
        train_end = t0.replace(year=year, month=month)

        month = train_end.month - (self.training_spec.window.lookback_months - self.training_spec.window.gap_months)
        year = train_end.year
        while month <= 0:
            month += 12
            year -= 1
        train_start = train_end.replace(year=year, month=month)
        return train_start, train_end

    def _read_windowed_input(self, name: str, train_start: datetime, train_end: datetime):
        from pyspark.sql import functions as F

        input_spec = self.training_spec.feature_inputs[name]
        logger.info("Reading %s from %s", name, input_spec.s3_prefix)
        df = self.spark.read.option("mergeSchema", "true").parquet(input_spec.s3_prefix)
        if "window_end_ts" not in df.columns:
            raise ValueError(f"{name} input must include window_end_ts")
        return df.filter((F.col("window_end_ts") >= F.lit(train_start)) & (F.col("window_end_ts") < F.lit(train_end)))

    def _preflight_validation(self, fg_a_df, fg_c_df, train_start: datetime, train_end: datetime) -> Dict[str, Any]:
        required_join_keys = self.training_spec.join_keys
        reliability = self.training_spec.reliability

        from pyspark.sql import functions as F

        expected_dates = {
            (train_start.date() + timedelta(days=offset)).isoformat()
            for offset in range(max((train_end.date() - train_start.date()).days, 1))
        }

        counts: Dict[str, int] = {}
        partition_details: Dict[str, Dict[str, Any]] = {}
        window_details: Dict[str, Dict[str, int]] = {}
        join_key_duplication: Dict[str, Dict[str, float]] = {}

        def _fail_preflight(reason: str, payload: Dict[str, Any]) -> None:
            self._write_preflight_failure_artifact(train_start=train_start, train_end=train_end, reason=reason, context=payload)
            raise ValueError(reason)

        for name, df in (("fg_a", fg_a_df), ("fg_c", fg_c_df)):
            missing = [key for key in required_join_keys if key not in df.columns]
            if missing:
                _fail_preflight(
                    f"{name} missing required join keys: {missing}",
                    {"dataset": name, "missing_join_keys": missing},
                )

            row_count = df.count()
            counts[name] = row_count
            if row_count < reliability.min_rows_per_input:
                _fail_preflight(
                    f"{name} row count {row_count} below minimum {reliability.min_rows_per_input}",
                    {"dataset": name, "row_count": row_count, "minimum": reliability.min_rows_per_input},
                )

            distinct_join_keys = df.select(*required_join_keys).distinct().count()
            duplication_ratio = row_count / max(distinct_join_keys, 1)
            join_key_duplication[name] = {
                "rows": float(row_count),
                "distinct_join_keys": float(distinct_join_keys),
                "rows_per_join_key": float(duplication_ratio),
            }
            if duplication_ratio > reliability.max_rows_per_join_key:
                _fail_preflight(
                    f"{name} rows-per-join-key {duplication_ratio:.4f} above maximum {reliability.max_rows_per_join_key:.4f}",
                    {
                        "dataset": name,
                        "rows_per_join_key": duplication_ratio,
                        "maximum_ratio": reliability.max_rows_per_join_key,
                        "distinct_join_keys": distinct_join_keys,
                    },
                )

            if "window_label" in df.columns:
                window_counts = {
                    str(row["window_label"]): int(row["count"])
                    for row in df.groupBy("window_label").count().collect()
                }
                window_details[name] = window_counts
                underfilled = [
                    {"window_label": label, "row_count": count}
                    for label, count in window_counts.items()
                    if count < reliability.min_rows_per_window
                ]
                if underfilled:
                    _fail_preflight(
                        f"{name} window_label coverage below minimum rows {reliability.min_rows_per_window}",
                        {
                            "dataset": name,
                            "minimum_rows_per_window": reliability.min_rows_per_window,
                            "underfilled_windows": underfilled,
                        },
                    )

            dt_frame = df.select(F.to_date(F.col("dt")).alias("dt")) if "dt" in df.columns else df.select(F.to_date(F.col("window_end_ts")).alias("dt"))
            observed_dates = {
                row[0].isoformat()
                for row in dt_frame.where(F.col("dt").isNotNull()).distinct().collect()
            }
            missing_partitions = sorted(expected_dates - observed_dates)
            observed_days = len(observed_dates)
            expected_days = len(expected_dates)
            coverage = observed_days / max(expected_days, 1)
            partition_details[name] = {
                "expected_days": expected_days,
                "observed_days": observed_days,
                "coverage_ratio": coverage,
                "missing_partition_days": len(missing_partitions),
                "missing_partitions": missing_partitions,
            }
            if coverage < reliability.min_partition_coverage_ratio:
                _fail_preflight(
                    f"{name} partition coverage {coverage:.4f} below minimum {reliability.min_partition_coverage_ratio:.4f}",
                    {
                        "dataset": name,
                        "partition_coverage": partition_details[name],
                        "minimum_ratio": reliability.min_partition_coverage_ratio,
                    },
                )

        if window_details.get("fg_a") and window_details.get("fg_c"):
            common_windows = sorted(set(window_details["fg_a"]).intersection(window_details["fg_c"]))
            if not common_windows:
                _fail_preflight(
                    "No overlapping window_label horizons between fg_a and fg_c",
                    {"fg_a_windows": sorted(window_details["fg_a"].keys()), "fg_c_windows": sorted(window_details["fg_c"].keys())},
                )

            underfilled_overlap = [
                {
                    "window_label": window,
                    "fg_a_rows": window_details["fg_a"].get(window, 0),
                    "fg_c_rows": window_details["fg_c"].get(window, 0),
                }
                for window in common_windows
                if min(window_details["fg_a"].get(window, 0), window_details["fg_c"].get(window, 0)) < reliability.min_rows_per_window
            ]
            if underfilled_overlap:
                _fail_preflight(
                    f"Overlapping windows below minimum rows {reliability.min_rows_per_window}",
                    {
                        "minimum_rows_per_window": reliability.min_rows_per_window,
                        "underfilled_overlapping_windows": underfilled_overlap,
                    },
                )

        fg_a_keys = fg_a_df.select(*required_join_keys).distinct()
        fg_c_keys = fg_c_df.select(*required_join_keys).distinct()
        join_keys_count = fg_a_keys.join(fg_c_keys, on=required_join_keys, how="inner").count()
        if join_keys_count < reliability.min_join_rows:
            _fail_preflight(
                f"join row count {join_keys_count} below minimum {reliability.min_join_rows}",
                {"join_key_count": join_keys_count, "minimum": reliability.min_join_rows},
            )

        coverage_ratio = join_keys_count / max(min(counts["fg_a"], counts["fg_c"]), 1)
        if coverage_ratio < reliability.min_join_coverage_ratio:
            _fail_preflight(
                f"join coverage ratio {coverage_ratio:.4f} below minimum {reliability.min_join_coverage_ratio:.4f}",
                {"join_coverage_ratio": coverage_ratio, "minimum_ratio": reliability.min_join_coverage_ratio},
            )

        return {
            "input_counts": counts,
            "join_key_count": join_keys_count,
            "join_coverage_ratio": coverage_ratio,
            "partition_coverage": partition_details,
            "window_coverage": window_details,
            "join_key_duplication": join_key_duplication,
            "expected_days": len(expected_dates),
            "join_keys": required_join_keys,
        }

    def _fit_preprocessing_params(self, df, feature_columns: List[str]) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
        from pyspark.sql import functions as F

        eps = self.training_spec.preprocessing.eps
        z_max = self.training_spec.preprocessing.z_max
        scaler_params: Dict[str, Dict[str, float]] = {}
        outlier_params: Dict[str, Dict[str, float]] = {}

        for feature in feature_columns:
            q = df.approxQuantile(feature, [0.25, 0.5, 0.75], 0.001)
            q = q if q else [0.0, 0.0, 0.0]
            median = float(q[1])
            iqr = float(q[2] - q[0])
            mad_df = df.select(F.abs(F.col(feature) - F.lit(median)).alias("mad_col"))
            mad_q = mad_df.approxQuantile("mad_col", [0.5], 0.001)
            mad = float(mad_q[0]) if mad_q else 0.0
            scaler_params[feature] = {"median": median, "iqr": iqr, "eps": eps}
            outlier_params[feature] = {"median": median, "mad": mad, "z_max": z_max, "eps": eps}

        return scaler_params, outlier_params

    def _apply_preprocessing_with_params(self, df, feature_columns: List[str], scaler_params, outlier_params):
        from pyspark.sql import functions as F

        out = df
        for feature in feature_columns:
            scaler = scaler_params.get(feature, {})
            outlier = outlier_params.get(feature, {})
            median = float(scaler.get("median", outlier.get("median", 0.0)))
            iqr = float(scaler.get("iqr", 1.0))
            mad = float(outlier.get("mad", 1.0))
            eps = float(scaler.get("eps", outlier.get("eps", self.training_spec.preprocessing.eps)))
            z_max = float(outlier.get("z_max", self.training_spec.preprocessing.z_max))
            robust_z = (F.col(feature) - F.lit(median)) / (F.lit(mad) + F.lit(eps))
            clipped = F.when(robust_z > z_max, F.lit(z_max)).when(robust_z < -z_max, F.lit(-z_max)).otherwise(robust_z)
            scaled = (F.col(feature) - F.lit(median)) / (F.lit(iqr) + F.lit(eps))

            strategy = (self.training_spec.preprocessing.imputation_strategy or "median_scaled").lower()
            if strategy == "none":
                imputed = F.coalesce(clipped, scaled)
            elif strategy == "constant":
                imputed = F.coalesce(clipped, scaled, F.lit(float(self.training_spec.preprocessing.imputation_constant)))
            else:
                # median_scaled => transformed median is 0.0
                imputed = F.coalesce(clipped, scaled, F.lit(0.0))

            out = out.withColumn(feature, imputed)
        return out

    def _select_features(self, train_processed, val_processed, feature_columns: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        from pyspark.sql import functions as F
        import numpy as np
        from sklearn.ensemble import IsolationForest

        if not self.training_spec.feature_selection.enabled:
            return feature_columns, {"steps": ["disabled"]}

        selected: List[str] = []
        for feature in feature_columns:
            variance = train_processed.select(F.var_pop(F.col(feature)).alias("var")).collect()[0]["var"]
            if variance is not None and float(variance) > self.training_spec.feature_selection.variance_threshold:
                selected.append(feature)

        pruned: List[str] = []
        for feature in selected:
            if not pruned:
                pruned.append(feature)
                continue
            max_corr = max(abs(train_processed.stat.corr(feature, kept) or 0.0) for kept in pruned)
            if max_corr <= self.training_spec.feature_selection.corr_threshold:
                pruned.append(feature)

        # Stability selection using bootstrap frequency of score-correlation signal.
        train_pdf = train_processed.select(*pruned).toPandas().fillna(0.0)
        stability_counts = {f: 0 for f in pruned}
        stability_runs = min(10, max(3, self.training_spec.tuning.max_trials // 2))
        if not train_pdf.empty and pruned:
            x = train_pdf.to_numpy(dtype=float)
            rng = np.random.default_rng(self.training_spec.random_seed)
            top_k = max(1, int(len(pruned) * 0.5))
            for run_idx in range(stability_runs):
                sample_idx = rng.integers(0, x.shape[0], size=x.shape[0])
                x_boot = x[sample_idx]
                model = IsolationForest(
                    n_estimators=100,
                    max_samples=1.0,
                    max_features=1.0,
                    contamination=0.01,
                    bootstrap=False,
                    random_state=self.training_spec.random_seed + run_idx,
                    n_jobs=1,
                ).fit(x_boot)
                scores = -model.score_samples(x_boot)
                importances = []
                for i, feature in enumerate(pruned):
                    col = x_boot[:, i]
                    if np.std(col) <= 1e-12:
                        importance = 0.0
                    else:
                        importance = abs(float(np.corrcoef(col, scores)[0, 1]))
                        if np.isnan(importance):
                            importance = 0.0
                    importances.append((feature, importance))
                for feat, _ in sorted(importances, key=lambda t: t[1], reverse=True)[:top_k]:
                    stability_counts[feat] += 1

        stability_selected = [
            feat for feat, cnt in stability_counts.items() if cnt / max(stability_runs, 1) >= 0.6
        ] or pruned

        # Permutation proxy on validation month.
        val_pdf = val_processed.select(*stability_selected).toPandas().fillna(0.0)
        permutation_selected = stability_selected
        permutation_impacts: Dict[str, float] = {f: 0.0 for f in stability_selected}
        if not val_pdf.empty and stability_selected:
            x_train = train_processed.select(*stability_selected).toPandas().fillna(0.0).to_numpy(dtype=float)
            x_val = val_pdf.to_numpy(dtype=float)
            ref_model = IsolationForest(
                n_estimators=100,
                max_samples=1.0,
                max_features=1.0,
                contamination=0.01,
                bootstrap=False,
                random_state=self.training_spec.random_seed,
                n_jobs=1,
            ).fit(x_train)
            base_scores = -ref_model.score_samples(x_val)
            rng = np.random.default_rng(self.training_spec.random_seed + 991)
            for i, feature in enumerate(stability_selected):
                perm = x_val.copy()
                perm[:, i] = perm[rng.permutation(perm.shape[0]), i]
                perm_scores = -ref_model.score_samples(perm)
                impact = float(np.mean(np.abs(base_scores - perm_scores)))
                permutation_impacts[feature] = impact
            impacts = list(permutation_impacts.values())
            threshold = np.percentile(impacts, 40) if impacts else 0.0
            permutation_selected = [f for f, imp in permutation_impacts.items() if imp >= threshold] or stability_selected

        meta = {
            "steps": ["near_constant", "correlation_pruning", "stability_selection", "permutation_proxy"],
            "stability_runs": stability_runs,
            "stability_counts": stability_counts,
            "permutation_impacts": permutation_impacts,
        }
        return permutation_selected, meta

    def _tune_and_validate(self, train_processed, val_processed, selected_features: List[str]):
        import numpy as np
        from sklearn.ensemble import IsolationForest
        from ndr.processing.bayesian_search_fallback import run_bayesian_search

        train_pdf = train_processed.select(*selected_features).toPandas().fillna(0.0)
        val_pdf = val_processed.select(*selected_features).toPandas().fillna(0.0)
        if train_pdf.empty or val_pdf.empty:
            raise ValueError("Training or validation dataset became empty after preprocessing")

        x_train = train_pdf.to_numpy(dtype=float)
        x_val = val_pdf.to_numpy(dtype=float)

        search_space = self.training_spec.tuning.search_space or {
            "n_estimators": [200, 400, 600, 800],
            "max_samples": [0.2, 0.4, 0.6, 0.8, 1.0],
            "max_features": [0.3, 0.5, 0.7, 1.0],
            "contamination": [0.001, 0.005, 0.01, 0.02, 0.05],
            "bootstrap": [False, True],
        }

        def eval_params(params: Dict[str, Any]) -> Dict[str, float]:
            model = IsolationForest(**params, random_state=self.training_spec.random_seed, n_jobs=1).fit(x_train)
            train_scores = -model.score_samples(x_train)
            val_scores = -model.score_samples(x_val)
            train_std = float(np.std(train_scores)) + 1e-6
            drift = abs(float(np.median(val_scores) - np.median(train_scores))) / train_std
            threshold = float(np.quantile(train_scores, 0.95))
            val_alert_rate = float((val_scores > threshold).mean())
            contamination = float(params.get("contamination", 0.01))
            alert_delta = abs(val_alert_rate - contamination)
            return {"objective": float(drift + alert_delta), "drift": float(drift), "alert_delta": float(alert_delta)}

        baseline_params = {
            "n_estimators": 200,
            "max_samples": 1.0,
            "max_features": 1.0,
            "contamination": 0.01,
            "bootstrap": False,
        }
        baseline_eval = eval_params(baseline_params)
        trials: List[Dict[str, Any]] = [{"trial": 0, "params": baseline_params, **baseline_eval}]

        hpo_method = "optuna_tpe"
        try:
            import optuna

            sampler = optuna.samplers.TPESampler(seed=self.training_spec.random_seed)
            study = optuna.create_study(direction="minimize", sampler=sampler)

            def objective(trial: optuna.trial.Trial) -> float:
                params = {
                    "n_estimators": trial.suggest_categorical("n_estimators", search_space["n_estimators"]),
                    "max_samples": trial.suggest_categorical("max_samples", search_space["max_samples"]),
                    "max_features": trial.suggest_categorical("max_features", search_space["max_features"]),
                    "contamination": trial.suggest_categorical("contamination", search_space["contamination"]),
                    "bootstrap": trial.suggest_categorical("bootstrap", search_space["bootstrap"]),
                }
                score = eval_params(params)
                trial.set_user_attr("params", params)
                trial.set_user_attr("drift", score["drift"])
                trial.set_user_attr("alert_delta", score["alert_delta"])
                return score["objective"]

            study.optimize(
                objective,
                n_trials=max(self.training_spec.tuning.max_trials - 1, 1),
                timeout=self.training_spec.tuning.timeout_seconds,
                gc_after_trial=True,
            )
            for t in study.trials:
                trials.append(
                    {
                        "trial": int(t.number) + 1,
                        "params": t.user_attrs.get("params", {}),
                        "objective": float(t.value),
                        "drift": float(t.user_attrs.get("drift", 0.0)),
                        "alert_delta": float(t.user_attrs.get("alert_delta", 0.0)),
                    }
                )
        except ImportError:
            trials, _ = run_bayesian_search(
                search_space=search_space,
                evaluate_fn=eval_params,
                max_trials=max(self.training_spec.tuning.max_trials, 1),
                seed=self.training_spec.random_seed,
                initial_params=baseline_params,
            )
            hpo_method = "local_bayesian_fallback"

        best_trial = min(trials, key=lambda t: t["objective"])
        baseline_obj = baseline_eval["objective"]
        best_obj = best_trial["objective"]
        rel_improvement = 0.0 if baseline_obj == 0 else (baseline_obj - best_obj) / abs(baseline_obj)

        ref_model = IsolationForest(**best_trial["params"], random_state=self.training_spec.random_seed, n_jobs=1).fit(x_train)
        train_scores = -ref_model.score_samples(x_train)
        val_scores = -ref_model.score_samples(x_val)
        alert_threshold = float(np.quantile(train_scores, 0.95))
        val_alert_rate = float((val_scores > alert_threshold).mean())
        alert_delta = abs(val_alert_rate - 0.05)
        score_drift = abs(float(np.median(val_scores) - np.median(train_scores))) / (float(np.std(train_scores)) + 1e-6)

        gates = {
            "min_relative_improvement": {
                "threshold": self.training_spec.validation_gates.min_relative_improvement,
                "value": rel_improvement,
                "passed": rel_improvement >= self.training_spec.validation_gates.min_relative_improvement,
            },
            "max_alert_volume_delta": {
                "threshold": self.training_spec.validation_gates.max_alert_volume_delta,
                "value": alert_delta,
                "passed": alert_delta <= self.training_spec.validation_gates.max_alert_volume_delta,
            },
            "max_score_drift": {
                "threshold": self.training_spec.validation_gates.max_score_drift,
                "value": score_drift,
                "passed": score_drift <= self.training_spec.validation_gates.max_score_drift,
            },
        }

        tuning_summary = {
            "method": hpo_method,
            "max_trials": self.training_spec.tuning.max_trials,
            "timeout_seconds": self.training_spec.tuning.timeout_seconds,
            "search_space": search_space,
            "trials": trials,
            "best_params": best_trial["params"],
            "baseline_objective": baseline_obj,
            "best_objective": best_obj,
            "relative_improvement": rel_improvement,
        }
        val_metrics = {
            "alert_threshold": alert_threshold,
            "val_alert_rate": val_alert_rate,
            "score_drift": score_drift,
        }
        return tuning_summary, best_trial["params"], gates, val_metrics

    def _fit_final_model(self, full_processed, selected_features: List[str], best_params: Dict[str, Any]):
        from sklearn.ensemble import IsolationForest

        full_pdf = full_processed.select(*selected_features).toPandas().fillna(0.0)
        x_full = full_pdf.to_numpy(dtype=float)
        if x_full.size == 0:
            raise ValueError("Full-window refit dataset is empty")
        return IsolationForest(**best_params, random_state=self.training_spec.random_seed, n_jobs=1).fit(x_full)

    def _build_metrics(
        self,
        full_processed,
        train_processed,
        val_processed,
        all_features: List[str],
        selected_features: List[str],
        tuning_summary: Dict[str, Any],
        gates: Dict[str, Any],
        val_metrics: Dict[str, Any],
    ) -> Dict[str, float]:
        return {
            "full_training_row_count": float(full_processed.count()),
            "train_row_count": float(train_processed.count()),
            "validation_row_count": float(val_processed.count()),
            "feature_count_pre_selection": float(len(all_features)),
            "feature_count_post_selection": float(len(selected_features)),
            "best_objective": float(tuning_summary["best_objective"]),
            "relative_improvement": float(tuning_summary["relative_improvement"]),
            "validation_alert_rate": float(val_metrics["val_alert_rate"]),
            "validation_score_drift": float(val_metrics["score_drift"]),
            "gates_passed": float(all(g["passed"] for g in gates.values())),
        }

    def _maybe_deploy(self, model_data_url: str, gates: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        deployment = self.training_spec.deployment
        if not deployment.deploy_on_success:
            return {"attempted": False, "status": "skipped", "reason": "deploy_on_success=false"}
        if not all(g["passed"] for g in gates.values()):
            return {"attempted": False, "status": "skipped", "reason": "promotion_gates_failed"}
        if not deployment.endpoint_name:
            return {"attempted": False, "status": "failed", "reason": "missing endpoint_name"}

        import boto3

        sm = boto3.client("sagemaker")
        endpoint_name = deployment.endpoint_name
        model_name = f"ndr-if-{self.training_spec.model_version}-{self.runtime_config.run_id}"
        endpoint_cfg = f"{endpoint_name}-cfg-{self.runtime_config.run_id}"

        previous_cfg = None
        endpoint_exists = False
        try:
            desc = sm.describe_endpoint(EndpointName=endpoint_name)
            previous_cfg = desc.get("EndpointConfigName")
            endpoint_exists = True
        except Exception:  # pylint: disable=broad-except
            endpoint_exists = False

        try:
            execution_role_arn = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")
            if not execution_role_arn:
                raise ValueError("SAGEMAKER_EXECUTION_ROLE_ARN is required for deployment")

            image_uri = os.environ.get("IF_INFERENCE_IMAGE_URI", "")
            if not image_uri:
                raise ValueError("IF_INFERENCE_IMAGE_URI is required for deployment")

            self._call_with_retries(
                sm.create_model,
                ModelName=model_name,
                ExecutionRoleArn=execution_role_arn,
                PrimaryContainer={"Image": image_uri, "ModelDataUrl": model_data_url},
            )
            self._call_with_retries(
                sm.create_endpoint_config,
                EndpointConfigName=endpoint_cfg,
                ProductionVariants=[
                    {
                        "VariantName": deployment.variant_name,
                        "ModelName": model_name,
                        "InstanceType": deployment.instance_type,
                        "InitialInstanceCount": deployment.initial_instance_count,
                    }
                ],
            )
            if endpoint_exists:
                self._call_with_retries(sm.update_endpoint, EndpointName=endpoint_name, EndpointConfigName=endpoint_cfg)
            else:
                self._call_with_retries(sm.create_endpoint, EndpointName=endpoint_name, EndpointConfigName=endpoint_cfg)

            return {
                "attempted": True,
                "status": "success",
                "endpoint_name": endpoint_name,
                "endpoint_config": endpoint_cfg,
                "previous_endpoint_config": previous_cfg,
                "rollback_triggered": False,
            }
        except Exception as exc:  # pylint: disable=broad-except
            rollback_triggered = False
            rollback_error = None
            if endpoint_exists and previous_cfg and deployment.rollback_on_alarm:
                try:
                    self._call_with_retries(sm.update_endpoint, EndpointName=endpoint_name, EndpointConfigName=previous_cfg)
                    rollback_triggered = True
                except Exception as rollback_exc:  # pylint: disable=broad-except
                    rollback_error = str(rollback_exc)
            return {
                "attempted": True,
                "status": "failed",
                "endpoint_name": endpoint_name,
                "endpoint_config": endpoint_cfg,
                "previous_endpoint_config": previous_cfg,
                "error": str(exc),
                "rollback_triggered": rollback_triggered,
                "rollback_error": rollback_error,
            }

    def _call_with_retries(self, fn, **kwargs):
        retries = max(self.training_spec.reliability.max_retries, 0)
        backoff = max(self.training_spec.reliability.backoff_seconds, 0.0)
        last_exc = None
        for attempt in range(retries + 1):
            try:
                return fn(**kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                if attempt >= retries:
                    break
                time.sleep(backoff * (2 ** attempt))
        raise last_exc

    def _evaluate_option4_guardrail(self) -> Dict[str, Any]:
        cfg = self.training_spec.cost_guardrail
        option1_monthly = cfg.option1_hourly_rate_usd * 24 * 30
        option4_hours = (cfg.option4_batch_minutes / 60.0) * cfg.option4_runs_per_month
        option4_monthly = option4_hours * cfg.option1_hourly_rate_usd
        review_required = cfg.enabled and option1_monthly > cfg.option1_monthly_budget_usd
        return {
            "enabled": cfg.enabled,
            "option1_estimated_monthly_usd": option1_monthly,
            "option4_estimated_monthly_usd": option4_monthly,
            "budget_threshold_usd": cfg.option1_monthly_budget_usd,
            "option4_review_required": review_required,
            "message": (
                "Review Option-4 batch inference trade-offs before switching deployment strategy"
                if review_required
                else "Option-1 estimated cost is within configured threshold"
            ),
        }

    def _persist_artifacts(
        self,
        scaler_params: Dict[str, Dict[str, float]],
        outlier_params: Dict[str, Dict[str, float]],
        feature_mask: List[str],
        metrics: Dict[str, float],
        tuning_summary: Dict[str, Any],
        model,
        train_start: datetime,
        train_end: datetime,
    ) -> Dict[str, str]:
        import boto3

        bucket, key_prefix = _split_s3_uri(self.training_spec.output.artifacts_s3_prefix)
        client = boto3.client("s3")

        run_prefix = (
            f"{key_prefix.rstrip('/')}/model_version={self.training_spec.model_version}/"
            f"training_start={train_start.strftime('%Y-%m-%d')}/"
            f"training_end={train_end.strftime('%Y-%m-%d')}/run_id={self.runtime_config.run_id}"
        )
        model_key = f"{run_prefix}/model/model.joblib"
        model_tar_key = f"{run_prefix}/model/model.tar.gz"

        model_hash = self._put_model_artifacts(client, bucket, model, model_key, model_tar_key)
        _put_json(client, bucket, f"{run_prefix}/preprocessing/scaler_params.json", scaler_params)
        _put_json(client, bucket, f"{run_prefix}/preprocessing/outlier_params.json", outlier_params)
        _put_json(client, bucket, f"{run_prefix}/preprocessing/feature_mask.json", feature_mask)
        _put_json(client, bucket, f"{run_prefix}/metrics/final_metrics.json", metrics)
        _put_json(client, bucket, f"{run_prefix}/metrics/tuning_metrics.json", tuning_summary)

        return {
            "bucket": bucket,
            "run_prefix": run_prefix,
            "model_key": model_key,
            "model_tar_key": model_tar_key,
            "model_hash": model_hash,
            "model_tar_s3_uri": f"s3://{bucket}/{model_tar_key}",
        }

    def _promote_latest_model_pointer(self, artifact_ctx: Dict[str, str]) -> str:
        import boto3

        bucket = artifact_ctx["bucket"]
        run_prefix = artifact_ctx["run_prefix"]
        key_prefix = run_prefix.split("/model_version=")[0]
        latest_model_tar_key = (
            f"{key_prefix}/model_version={self.training_spec.model_version}/latest/model/model.tar.gz"
        )
        client = boto3.client("s3")
        client.copy_object(
            Bucket=bucket,
            Key=latest_model_tar_key,
            CopySource={"Bucket": bucket, "Key": artifact_ctx["model_tar_key"]},
        )
        return f"s3://{bucket}/{latest_model_tar_key}"

    def _write_final_report_and_success(
        self,
        train_start: datetime,
        train_end: datetime,
        preflight: Dict[str, Any],
        metadata_columns: List[str],
        selected_features: List[str],
        scaler_params: Dict[str, Dict[str, float]],
        outlier_params: Dict[str, Dict[str, float]],
        tuning_summary: Dict[str, Any],
        metrics: Dict[str, float],
        gates: Dict[str, Any],
        deployment_status: Dict[str, Any],
        selection_meta: Dict[str, Any],
        artifact_ctx: Dict[str, str],
        latest_model_path: str | None,
    ) -> str:
        import boto3
        import sklearn

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        client = boto3.client("s3")

        report = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
                "random_seed": self.training_spec.random_seed,
            },
            "input_datasets": {
                "fg_a_prefix": self.training_spec.feature_inputs["fg_a"].s3_prefix,
                "fg_c_prefix": self.training_spec.feature_inputs["fg_c"].s3_prefix,
                "preflight": preflight,
            },
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
                "lookback_months": self.training_spec.window.lookback_months,
                "gap_months": self.training_spec.window.gap_months,
            },
            "schema_and_join": {
                "fg_a_manifest": "build_fg_a_manifest",
                "fg_c_manifest": "build_fg_c_manifest",
                "join_keys": self.training_spec.join_keys,
                "metadata_columns": metadata_columns,
                "selected_feature_columns": selected_features,
            },
            "preprocessing": {
                "eps": self.training_spec.preprocessing.eps,
                "z_max": self.training_spec.preprocessing.z_max,
                "imputation_strategy": self.training_spec.preprocessing.imputation_strategy,
                "imputation_constant": self.training_spec.preprocessing.imputation_constant,
                "scaler_params": scaler_params,
                "outlier_params": outlier_params,
            },
            "feature_selection": {
                **asdict(self.training_spec.feature_selection),
                "selection_meta": selection_meta,
            },
            "tuning": tuning_summary,
            "final_model": {
                "model_path": f"s3://{artifact_ctx['bucket']}/{artifact_ctx['model_key']}",
                "model_image_copy_path": f"s3://{artifact_ctx['bucket']}/{artifact_ctx['model_tar_key']}",
                "latest_model_image_path": latest_model_path,
                "artifact_hash": artifact_ctx["model_hash"],
            },
            "deployment": {**asdict(self.training_spec.deployment), "status": deployment_status},
            "cost_guardrail": self._evaluate_option4_guardrail(),
            "validation_gates": gates,
            "metrics": metrics,
            "deterministic_context": {
                "python_version": os.sys.version,
                "sklearn_version": sklearn.__version__,
                "output_prefix": f"s3://{artifact_ctx['bucket']}/{artifact_ctx['run_prefix']}",
                "runtime_env": {
                    "table_env": os.environ.get(DDB_TABLE_ENV_VAR) or os.environ.get(LEGACY_DDB_TABLE_ENV_VAR),
                },
                "resolved_job_spec_payload": self.resolved_spec_payload,
            },
        }

        report_key = f"{report_key_prefix.rstrip('/')}/run_id={self.runtime_config.run_id}/final_training_report.json"
        _put_json(client, report_bucket, report_key, report)

        self._write_inference_preprocessing_back(selected_features, scaler_params, outlier_params)
        client.put_object(Bucket=artifact_ctx["bucket"], Key=f"{artifact_ctx['run_prefix']}/SUCCESS", Body=b"ok")
        return f"s3://{report_bucket}/{report_key}"

    def _write_preflight_failure_artifact(
        self,
        train_start: datetime,
        train_end: datetime,
        reason: str,
        context: Dict[str, Any],
    ) -> None:
        import boto3

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        key = f"{report_key_prefix.rstrip('/')}/run_id={self.runtime_config.run_id}/preflight_failure_context.json"
        payload = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
            },
            "failure": {
                "stage": "preflight",
                "reason": reason,
                "context": context,
            },
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
            },
            "resolved_job_spec_payload": self.resolved_spec_payload,
        }
        try:
            client = boto3.client("s3")
            _put_json(client, report_bucket, key, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to write preflight failure artifact: %s", exc)

    def _write_failure_report(self, stage: str, error: str, train_start: datetime, train_end: datetime) -> None:
        import boto3

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        key = f"{report_key_prefix.rstrip('/')}/run_id={self.runtime_config.run_id}/failure_report.json"
        payload = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
            },
            "failure": {
                "stage": stage,
                "error": error,
            },
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
            },
            "resolved_job_spec_payload": self.resolved_spec_payload,
        }
        try:
            client = boto3.client("s3")
            _put_json(client, report_bucket, key, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to write failure report: %s", exc)

    def _write_failure_experiment_artifact(
        self,
        stage: str,
        error: str,
        train_start: datetime,
        train_end: datetime,
    ) -> None:
        import boto3

        report_bucket, report_key_prefix = _split_s3_uri(self.training_spec.output.report_s3_prefix)
        context_key = (
            f"{report_key_prefix.rstrip('/')}/run_id={self.runtime_config.run_id}/"
            "failure_experiment_context.json"
        )
        payload = {
            "run_metadata": {
                "project_name": self.runtime_config.project_name,
                "feature_spec_version": self.runtime_config.feature_spec_version,
                "model_version": self.training_spec.model_version,
                "run_id": self.runtime_config.run_id,
                "execution_ts": self.runtime_config.execution_ts_iso,
            },
            "failure": {"stage": stage, "error": error},
            "training_window": {
                "start": train_start.isoformat(),
                "end": train_end.isoformat(),
            },
            "resolved_job_spec_payload": self.resolved_spec_payload,
        }

        try:
            s3 = boto3.client("s3")
            _put_json(s3, report_bucket, context_key, payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to persist failure experiment context: %s", exc)
            return

        if not self.training_spec.experiments.enabled:
            return

        try:
            sm = boto3.client("sagemaker")
            experiment_name = self.training_spec.experiments.experiment_name or f"{self.runtime_config.project_name}-if-training"
            trial_name = (
                f"{self.training_spec.experiments.trial_prefix}-{self.runtime_config.feature_spec_version}-{self.runtime_config.run_id}"
            )
            component_name = f"{trial_name}-failure"
            _safe_create_experiment(sm, experiment_name)
            _safe_create_trial(sm, trial_name, experiment_name)
            _safe_create_trial_component(sm, component_name)
            sm.associate_trial_component(TrialComponentName=component_name, TrialName=trial_name)
            sm.update_trial_component(
                TrialComponentName=component_name,
                Parameters={
                    "failure_stage": {"StringValue": stage},
                    "failure_error": {"StringValue": error[:1024]},
                    "failure_context_s3_uri": {
                        "StringValue": f"s3://{report_bucket}/{context_key}"
                    },
                },
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to update failure experiment artifact: %s", exc)

    def _log_sagemaker_experiments(
        self,
        metrics: Dict[str, float],
        tuning_summary: Dict[str, Any],
        deployment_status: Dict[str, Any],
        preflight: Dict[str, Any],
        report_s3_uri: str,
        selected_feature_count: int,
        all_feature_count: int,
    ) -> None:
        if not self.training_spec.experiments.enabled:
            return
        try:
            import boto3

            sm = boto3.client("sagemaker")
            experiment_name = self.training_spec.experiments.experiment_name or f"{self.runtime_config.project_name}-if-training"
            trial_name = (
                f"{self.training_spec.experiments.trial_prefix}-{self.runtime_config.feature_spec_version}-{self.runtime_config.run_id}"
            )
            preprocessing_component = f"{trial_name}-preprocessing"
            tuning_component = f"{trial_name}-tuning"
            final_component = f"{trial_name}-final"

            _safe_create_experiment(sm, experiment_name)
            _safe_create_trial(sm, trial_name, experiment_name)
            for component_name in (preprocessing_component, tuning_component, final_component):
                _safe_create_trial_component(sm, component_name)
                sm.associate_trial_component(TrialComponentName=component_name, TrialName=trial_name)

            sm.update_trial_component(
                TrialComponentName=preprocessing_component,
                Parameters={
                    "feature_count_before_selection": {"StringValue": str(all_feature_count)},
                    "feature_count_after_selection": {"StringValue": str(selected_feature_count)},
                    "preflight_join_coverage_ratio": {"StringValue": f"{float(preflight.get('join_coverage_ratio', 0.0)):.6f}"},
                    "preflight_partition_coverage": {"StringValue": json.dumps(preflight.get("partition_coverage", {}), sort_keys=True)[:1024]},
                },
            )

            tuning_metrics = [
                {
                    "MetricName": "best_objective",
                    "Value": float(tuning_summary.get("best_objective", 0.0)),
                    "Timestamp": datetime.now(timezone.utc),
                },
                {
                    "MetricName": "baseline_objective",
                    "Value": float(tuning_summary.get("baseline_objective", 0.0)),
                    "Timestamp": datetime.now(timezone.utc),
                },
                {
                    "MetricName": "objective_improvement",
                    "Value": float(tuning_summary.get("objective_improvement", 0.0)),
                    "Timestamp": datetime.now(timezone.utc),
                },
            ]
            sm.batch_put_metrics(TrialComponentName=tuning_component, MetricData=tuning_metrics)

            final_metrics = [
                {"MetricName": k, "Value": float(v), "Timestamp": datetime.now(timezone.utc)}
                for k, v in metrics.items()
            ] + [
                {
                    "MetricName": "deployment_attempted",
                    "Value": 1.0 if deployment_status.get("attempted") else 0.0,
                    "Timestamp": datetime.now(timezone.utc),
                },
            ]
            sm.batch_put_metrics(TrialComponentName=final_component, MetricData=final_metrics)
            sm.update_trial_component(
                TrialComponentName=final_component,
                Parameters={
                    "final_report_s3_uri": {"StringValue": report_s3_uri},
                    "deployment_status": {"StringValue": str(deployment_status.get("status", "unknown"))},
                },
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to log SageMaker Experiments metrics: %s", exc)

    def _write_inference_preprocessing_back(
        self,
        feature_mask: List[str],
        scaler_params: Dict[str, Dict[str, float]],
        outlier_params: Dict[str, Dict[str, float]],
    ) -> None:
        import boto3

        table_name = os.environ.get(DDB_TABLE_ENV_VAR) or os.environ.get(LEGACY_DDB_TABLE_ENV_VAR)
        if not table_name:
            logger.warning("Skipping inference spec update: DynamoDB table env var not set")
            return

        ddb = boto3.resource("dynamodb")
        table = ddb.Table(table_name)
        job_name = f"inference_predictions#{self.runtime_config.feature_spec_version}"
        item = table.get_item(Key={"project_name": self.runtime_config.project_name, "job_name": job_name}).get("Item")
        if not item:
            logger.warning("Skipping inference spec update: %s not found", job_name)
            return

        spec = item.get("spec", {})
        payload = spec.get("payload", {})
        payload["feature_columns"] = feature_mask
        payload["scaler_params"] = scaler_params
        payload["outlier_params"] = outlier_params
        payload["model_version"] = self.training_spec.model_version
        spec["payload"] = payload

        table.update_item(
            Key={"project_name": self.runtime_config.project_name, "job_name": job_name},
            UpdateExpression="SET #spec = :spec",
            ExpressionAttributeNames={"#spec": "spec"},
            ExpressionAttributeValues={":spec": spec},
        )

    def _put_model_artifacts(self, client, bucket: str, model, model_key: str, model_tar_key: str) -> str:
        import joblib

        model_buffer = io.BytesIO()
        joblib.dump(model, model_buffer)
        model_bytes = model_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=model_key, Body=model_bytes)

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            file_data = io.BytesIO(model_bytes)
            info = tarfile.TarInfo(name="model.joblib")
            info.size = len(model_bytes)
            tar.addfile(info, file_data)
        client.put_object(Bucket=bucket, Key=model_tar_key, Body=tar_buffer.getvalue())

        return hashlib.sha256(model_bytes).hexdigest()


def _safe_create_experiment(client, experiment_name: str) -> None:
    try:
        client.create_experiment(ExperimentName=experiment_name)
    except Exception as exc:  # pylint: disable=broad-except
        if "already exists" not in str(exc).lower():
            raise


def _safe_create_trial(client, trial_name: str, experiment_name: str) -> None:
    try:
        client.create_trial(TrialName=trial_name, ExperimentName=experiment_name)
    except Exception as exc:  # pylint: disable=broad-except
        if "already exists" not in str(exc).lower():
            raise


def _safe_create_trial_component(client, component_name: str) -> None:
    try:
        client.create_trial_component(TrialComponentName=component_name)
    except Exception as exc:  # pylint: disable=broad-except
        if "already exists" not in str(exc).lower():
            raise


def _split_s3_uri(uri: str) -> Tuple[str, str]:
    cleaned = uri.replace("s3://", "", 1)
    bucket, _, key = cleaned.partition("/")
    return bucket, key


def _put_json(client, bucket: str, key: str, payload) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def run_if_training_from_runtime_config(runtime_config: IFTrainingRuntimeConfig) -> None:
    from ndr.config.job_spec_loader import load_job_spec
    from pyspark.sql import SparkSession

    job_spec = load_job_spec(
        project_name=runtime_config.project_name,
        job_name="if_training",
        feature_spec_version=runtime_config.feature_spec_version,
    )
    training_spec = parse_if_training_spec(job_spec)
    spark = SparkSession.builder.getOrCreate()
    IFTrainingJob(spark, runtime_config, training_spec, resolved_spec_payload=job_spec).run()
