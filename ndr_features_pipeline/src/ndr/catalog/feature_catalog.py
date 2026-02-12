"""Feature catalog builder for NDR feature groups."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

from ndr.model.fg_a_schema import (
    DERIVED_METRICS,
    HIGH_RISK_SEGMENT_METRICS,
    NOVELTY_METRICS,
    OUTBOUND_BASE_METRICS,
    WINDOW_LABELS,
    build_feature_name,
)
from ndr.model.fg_c_schema import DEFAULT_TRANSFORMS, build_default_metric_list


@dataclass(frozen=True)
class FeatureDefinition:
    """Data container for FeatureDefinition."""
    feature_id: str
    feature_number: int
    feature_name: str
    feature_group: str
    feature_formula: str
    feature_description: str
    creator_script: str
    upstream_dependencies: Sequence[str]
    feature_spec_version: str
    data_type: str
    nullable: bool
    default_value: Optional[str]
    downstream_consumers: Sequence[str]
    feature_group_name_offline: Optional[str]
    feature_group_name_online: Optional[str]
    s3_storage_path: Optional[str]

    def to_dict(self) -> dict[str, object]:
        """Execute the to dict stage of the workflow."""
        return {
            "feature_id": self.feature_id,
            "feature_number": self.feature_number,
            "feature_name": self.feature_name,
            "feature_group": self.feature_group,
            "feature_formula": self.feature_formula,
            "feature_description": self.feature_description,
            "creator_script": self.creator_script,
            "upstream_dependencies": list(self.upstream_dependencies),
            "feature_spec_version": self.feature_spec_version,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "default_value": self.default_value,
            "downstream_consumers": list(self.downstream_consumers),
            "feature_group_name_offline": self.feature_group_name_offline,
            "feature_group_name_online": self.feature_group_name_online,
            "s3_storage_path": self.s3_storage_path,
        }


@dataclass(frozen=True)
class CatalogBundle:
    """Data container for CatalogBundle."""
    model_inputs: List[FeatureDefinition]
    non_model_fields: List[FeatureDefinition]


def build_fg_a_metric_names() -> List[str]:
    """Execute the build fg a metric names stage of the workflow."""
    metric_families = (
        OUTBOUND_BASE_METRICS
        + DERIVED_METRICS
        + NOVELTY_METRICS
        + HIGH_RISK_SEGMENT_METRICS
    )
    prefixes = ["", "in_"]
    names: List[str] = []
    for prefix in prefixes:
        for metric in metric_families:
            for window_label in WINDOW_LABELS:
                names.append(build_feature_name(prefix, metric, window_label))
    return names


def build_fg_c_metric_names(
    metrics: Optional[Sequence[str]] = None,
    transforms: Optional[Sequence[str]] = None,
) -> List[str]:
    """Execute the build fg c metric names stage of the workflow."""
    base_metrics = list(metrics) if metrics is not None else build_default_metric_list()
    transform_list = list(transforms) if transforms is not None else DEFAULT_TRANSFORMS
    features: List[str] = []
    for metric in base_metrics:
        for transform in transform_list:
            features.append(f"{metric}_{transform}")
    return features


def _feature_id(feature_spec_version: str, feature_group: str, feature_name: str) -> str:
    """Execute the feature id stage of the workflow."""
    return f"{feature_spec_version}:{feature_group}:{feature_name}"


def _metric_description(metric_name: str, feature_group: str) -> str:
    """Execute the metric description stage of the workflow."""
    return f"{feature_group} feature derived for {metric_name}."


def _build_feature_defs(
    feature_spec_version: str,
    feature_group: str,
    feature_names: Iterable[str],
    creator_script: str,
    upstream_dependencies: Sequence[str],
    downstream_consumers: Sequence[str],
    data_type: str = "double",
    feature_group_name_offline: Optional[str] = None,
    feature_group_name_online: Optional[str] = None,
    s3_storage_path: Optional[str] = None,
    start_index: int = 1,
) -> List[FeatureDefinition]:
    """Execute the build feature defs stage of the workflow."""
    entries: List[FeatureDefinition] = []
    counter = start_index
    for name in feature_names:
        entries.append(
            FeatureDefinition(
                feature_id=_feature_id(feature_spec_version, feature_group, name),
                feature_number=counter,
                feature_name=name,
                feature_group=feature_group,
                feature_formula=f"Derived in {feature_group}.",
                feature_description=_metric_description(name, feature_group),
                creator_script=creator_script,
                upstream_dependencies=upstream_dependencies,
                feature_spec_version=feature_spec_version,
                data_type=data_type,
                nullable=True,
                default_value=None,
                downstream_consumers=downstream_consumers,
                feature_group_name_offline=feature_group_name_offline,
                feature_group_name_online=feature_group_name_online,
                s3_storage_path=s3_storage_path,
            )
        )
        counter += 1
    return entries


def build_feature_catalog(
    feature_spec_version: str,
    fg_a_offline_name: Optional[str] = None,
    fg_a_online_name: Optional[str] = None,
    fg_c_offline_name: Optional[str] = None,
    fg_c_online_name: Optional[str] = None,
    fg_a_s3_path: Optional[str] = None,
    fg_c_s3_path: Optional[str] = None,
) -> CatalogBundle:
    """Build model-input and non-model feature catalogs for a spec version."""
    fg_a_metric_features = build_fg_a_metric_names()
    fg_a_context = [
        "hour_of_day",
        "is_working_hours",
        "is_off_hours",
        "is_night_hours",
        "is_weekend",
    ]
    fg_a_features = fg_a_metric_features + fg_a_context

    fg_c_metrics = build_default_metric_list()
    fg_c_features = build_fg_c_metric_names(metrics=fg_c_metrics)

    model_features: List[FeatureDefinition] = []
    model_features.extend(
        _build_feature_defs(
            feature_spec_version=feature_spec_version,
            feature_group="fg_a",
            feature_names=fg_a_features,
            creator_script="src/ndr/processing/fg_a_builder_job.py",
            upstream_dependencies=["delta_builder"],
            downstream_consumers=["NDR system"],
            feature_group_name_offline=fg_a_offline_name,
            feature_group_name_online=fg_a_online_name,
            s3_storage_path=fg_a_s3_path,
            start_index=1,
        )
    )

    model_features.extend(
        _build_feature_defs(
            feature_spec_version=feature_spec_version,
            feature_group="fg_c",
            feature_names=fg_c_features,
            creator_script="src/ndr/processing/fg_c_builder_job.py",
            upstream_dependencies=["fg_a_builder", "fg_b_builder", "pair_counts_builder"],
            downstream_consumers=["NDR system"],
            feature_group_name_offline=fg_c_offline_name,
            feature_group_name_online=fg_c_online_name,
            s3_storage_path=fg_c_s3_path,
            start_index=len(model_features) + 1,
        )
    )

    non_model_features: List[FeatureDefinition] = []

    delta_fields = [
        "host_ip",
        "role",
        "slice_start_ts",
        "slice_end_ts",
        "dt",
        "hh",
        "mm",
        "sessions_cnt",
        "bytes_sent_sum",
        "bytes_recv_sum",
        "duration_sum",
        "allow_cnt",
        "drop_cnt",
        "alert_cnt",
        "rst_cnt",
        "aged_out_cnt",
        "threat_cnt",
        "traffic_cnt",
        "tcp_cnt",
        "udp_cnt",
        "icmp_cnt",
        "max_sessions_per_minute",
    ]
    delta_types = {
        "host_ip": "string",
        "role": "string",
        "slice_start_ts": "timestamp",
        "slice_end_ts": "timestamp",
        "dt": "string",
        "hh": "string",
        "mm": "string",
    }
    for field in delta_fields:
        data_type = delta_types.get(field, "double")
        non_model_features.extend(
            _build_feature_defs(
                feature_spec_version=feature_spec_version,
                feature_group="delta",
                feature_names=[field],
                creator_script="src/ndr/processing/delta_builder_job.py",
                upstream_dependencies=["raw_logs"],
                downstream_consumers=["fg_a_builder"],
                data_type=data_type,
                start_index=len(non_model_features) + 1,
            )
        )

    baseline_metrics = build_fg_a_metric_names()
    baseline_suffixes = [
        "median",
        "p25",
        "p75",
        "p95",
        "p99",
        "mad",
        "iqr",
        "support_count",
        "cold_start_flag",
    ]
    fg_b_fields = [f"{m}_{suffix}" for m in baseline_metrics for suffix in baseline_suffixes]
    fg_b_fields.extend(
        [
            "host_ip",
            "segment_id",
            "role",
            "time_band",
            "window_label",
            "baseline_horizon",
            "baseline_start_ts",
            "baseline_end_ts",
            "record_id",
        ]
    )
    fg_b_types = {
        "host_ip": "string",
        "segment_id": "string",
        "role": "string",
        "time_band": "string",
        "window_label": "string",
        "baseline_horizon": "string",
        "baseline_start_ts": "string",
        "baseline_end_ts": "string",
        "record_id": "string",
    }
    for field in fg_b_fields:
        non_model_features.extend(
            _build_feature_defs(
                feature_spec_version=feature_spec_version,
                feature_group="fg_b_baselines",
                feature_names=[field],
                creator_script="src/ndr/processing/fg_b_builder_job.py",
                upstream_dependencies=["fg_a_builder"],
                downstream_consumers=["fg_c_builder"],
                data_type=fg_b_types.get(field, "double"),
                start_index=len(non_model_features) + 1,
            )
        )

    pair_counts_fields = [
        "src_ip",
        "dst_ip",
        "dst_port",
        "segment_id",
        "event_ts",
        "sessions_cnt",
    ]
    pair_counts_types = {
        "src_ip": "string",
        "dst_ip": "string",
        "dst_port": "int",
        "segment_id": "string",
        "event_ts": "timestamp",
        "sessions_cnt": "long",
    }
    for field in pair_counts_fields:
        non_model_features.extend(
            _build_feature_defs(
                feature_spec_version=feature_spec_version,
                feature_group="pair_counts",
                feature_names=[field],
                creator_script="src/ndr/processing/pair_counts_builder_job.py",
                upstream_dependencies=["raw_logs"],
                downstream_consumers=["fg_b_builder"],
                data_type=pair_counts_types[field],
                start_index=len(non_model_features) + 1,
            )
        )

    pair_rarity_fields = [
        "pair_seen_count",
        "pair_last_seen_ts",
        "active_days",
        "pair_daily_avg",
        "pair_rarity_score",
        "is_new_pair_flag",
        "is_rare_pair_flag",
        "baseline_horizon",
    ]
    pair_rarity_types = {
        "pair_seen_count": "long",
        "pair_last_seen_ts": "timestamp",
        "active_days": "long",
        "pair_daily_avg": "double",
        "pair_rarity_score": "double",
        "is_new_pair_flag": "int",
        "is_rare_pair_flag": "int",
        "baseline_horizon": "string",
    }
    for field in pair_rarity_fields:
        non_model_features.extend(
            _build_feature_defs(
                feature_spec_version=feature_spec_version,
                feature_group="pair_rarity_baselines",
                feature_names=[field],
                creator_script="src/ndr/processing/fg_b_builder_job.py",
                upstream_dependencies=["pair_counts_builder"],
                downstream_consumers=["fg_c_builder"],
                data_type=pair_rarity_types[field],
                start_index=len(non_model_features) + 1,
            )
        )

    return CatalogBundle(model_inputs=model_features, non_model_fields=non_model_features)

