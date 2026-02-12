"""Schema manifest definitions for feature outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from ndr.catalog.feature_catalog import build_fg_a_metric_names, build_fg_c_metric_names
from ndr.model.fg_c_schema import DEFAULT_TRANSFORMS, build_default_metric_list


@dataclass(frozen=True)
class SchemaField:
    """Data container for SchemaField."""
    name: str
    data_type: str
    nullable: bool = True
    default_value: object | None = None


@dataclass(frozen=True)
class SchemaManifest:
    """Data container for SchemaManifest."""
    name: str
    fields: Sequence[SchemaField]

    @property
    def field_names(self) -> List[str]:
        """Execute the field names stage of the workflow."""
        return [field.name for field in self.fields]


PAIR_RARITY_FIELDS = [
    "pair_seen_count",
    "pair_last_seen_ts",
    "active_days",
    "pair_daily_avg",
    "pair_rarity_score",
    "is_new_pair_flag",
    "is_rare_pair_flag",
]


def build_delta_manifest(port_set_names: Sequence[str] | None = None) -> SchemaManifest:
    """Execute the build delta manifest stage of the workflow."""
    base_fields = [
        SchemaField("host_ip", "string", False),
        SchemaField("role", "string", False),
        SchemaField("slice_start_ts", "timestamp", False),
        SchemaField("slice_end_ts", "timestamp", False),
        SchemaField("dt", "string", False),
        SchemaField("hh", "string", False),
        SchemaField("mm", "string", False),
        SchemaField("sessions_cnt", "long"),
        SchemaField("bytes_sent_sum", "double"),
        SchemaField("bytes_recv_sum", "double"),
        SchemaField("duration_sum", "double"),
        SchemaField("allow_cnt", "long"),
        SchemaField("drop_cnt", "long"),
        SchemaField("alert_cnt", "long"),
        SchemaField("rst_cnt", "long"),
        SchemaField("aged_out_cnt", "long"),
        SchemaField("threat_cnt", "long"),
        SchemaField("traffic_cnt", "long"),
        SchemaField("tcp_cnt", "long"),
        SchemaField("udp_cnt", "long"),
        SchemaField("icmp_cnt", "long"),
        SchemaField("max_sessions_per_minute", "long"),
    ]
    extra = []
    for name in port_set_names or []:
        extra.append(SchemaField(f"{name.lower()}_sessions_cnt", "long"))
    return SchemaManifest(name="delta", fields=base_fields + extra)


def build_fg_a_manifest() -> SchemaManifest:
    """Execute the build fg a manifest stage of the workflow."""
    metric_fields = [SchemaField(name, "double") for name in build_fg_a_metric_names()]
    metadata_fields = [
        SchemaField("host_ip", "string", False),
        SchemaField("window_label", "string", False),
        SchemaField("window_start_ts", "timestamp", False),
        SchemaField("window_end_ts", "timestamp", False),
        SchemaField("record_id", "string", False),
        SchemaField("mini_batch_id", "string", False),
        SchemaField("feature_spec_version", "string", False),
        SchemaField("hour_of_day", "int"),
        SchemaField("day_of_week", "string"),
        SchemaField("is_working_hours", "int"),
        SchemaField("is_off_hours", "int"),
        SchemaField("is_night_hours", "int"),
        SchemaField("is_weekend", "int"),
    ]
    return SchemaManifest(name="fg_a", fields=metadata_fields + metric_fields)


def build_fg_b_manifest(metrics: Sequence[str], include_record_id: bool) -> SchemaManifest:
    """Execute the build fg b manifest stage of the workflow."""
    suffixes = [
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
    metric_fields = [
        SchemaField(f"{metric}_{suffix}", "double") for metric in metrics for suffix in suffixes
    ]
    meta_fields = [
        SchemaField("baseline_horizon", "string", False),
        SchemaField("baseline_start_ts", "string", False),
        SchemaField("baseline_end_ts", "string", False),
    ]
    if include_record_id:
        meta_fields.append(SchemaField("record_id", "string", False))
    return SchemaManifest(name="fg_b_baselines", fields=meta_fields + metric_fields)


def build_fg_b_host_manifest(metrics: Sequence[str]) -> SchemaManifest:
    """Execute the build fg b host manifest stage of the workflow."""
    fields = [
        SchemaField("host_ip", "string", False),
        SchemaField("role", "string", False),
        SchemaField("segment_id", "string", False),
        SchemaField("time_band", "string", False),
        SchemaField("window_label", "string", False),
    ]
    return SchemaManifest(
        name="fg_b_host",
        fields=fields + list(build_fg_b_manifest(metrics, include_record_id=True).fields),
    )


def build_fg_b_segment_manifest(metrics: Sequence[str]) -> SchemaManifest:
    """Execute the build fg b segment manifest stage of the workflow."""
    fields = [
        SchemaField("segment_id", "string", False),
        SchemaField("role", "string", False),
        SchemaField("time_band", "string", False),
        SchemaField("window_label", "string", False),
    ]
    return SchemaManifest(
        name="fg_b_segment",
        fields=fields + list(build_fg_b_manifest(metrics, include_record_id=False).fields),
    )


def build_fg_b_ip_metadata_manifest() -> SchemaManifest:
    """Execute the build fg b ip metadata manifest stage of the workflow."""
    fields = [
        SchemaField("host_ip", "string", False),
        SchemaField("window_label", "string", False),
        SchemaField("baseline_horizon", "string", False),
        SchemaField("is_full_history", "int"),
        SchemaField("is_non_persistent_machine", "int"),
        SchemaField("is_cold_start", "int"),
    ]
    return SchemaManifest(name="fg_b_ip_metadata", fields=fields)


def build_pair_counts_manifest() -> SchemaManifest:
    """Execute the build pair counts manifest stage of the workflow."""
    fields = [
        SchemaField("src_ip", "string", False),
        SchemaField("dst_ip", "string", False),
        SchemaField("dst_port", "int"),
        SchemaField("segment_id", "string"),
        SchemaField("event_ts", "timestamp", False),
        SchemaField("sessions_cnt", "long"),
        SchemaField("mini_batch_id", "string", False),
        SchemaField("feature_spec_version", "string", False),
        SchemaField("dt", "date", False),
        SchemaField("hh", "string", False),
        SchemaField("mm", "string", False),
    ]
    return SchemaManifest(name="pair_counts", fields=fields)


def build_pair_rarity_manifest(group_keys: Sequence[str]) -> SchemaManifest:
    """Execute the build pair rarity manifest stage of the workflow."""
    base_fields = [SchemaField(key, "string", False) for key in group_keys]
    metric_fields = []
    for name in PAIR_RARITY_FIELDS:
        if name == "pair_last_seen_ts":
            metric_fields.append(SchemaField(name, "timestamp"))
        elif name in {"pair_seen_count", "active_days", "is_new_pair_flag", "is_rare_pair_flag"}:
            metric_fields.append(SchemaField(name, "long"))
        else:
            metric_fields.append(SchemaField(name, "double"))
    metric_fields.append(SchemaField("baseline_horizon", "string", False))
    return SchemaManifest(
        name="pair_rarity",
        fields=base_fields + metric_fields,
    )


def build_fg_c_manifest(
    metrics: Sequence[str],
    transforms: Sequence[str] | None = None,
    suspicion_metrics: Sequence[str] | None = None,
    rare_pair_metrics: Sequence[str] | None = None,
    include_time_band_violation: bool = False,
) -> SchemaManifest:
    """Execute the build fg c manifest stage of the workflow."""
    transform_list = list(transforms) if transforms is not None else DEFAULT_TRANSFORMS
    feature_names = build_fg_c_metric_names(metrics=metrics, transforms=transform_list)

    correlation_fields = [SchemaField(name, "double") for name in feature_names]
    suspicion_fields: List[SchemaField] = []
    suspicion_metric_list = list(suspicion_metrics or [])
    for metric in suspicion_metric_list:
        suspicion_fields.extend(
            [
                SchemaField(f"{metric}_excess_over_p95", "double"),
                SchemaField(f"{metric}_excess_ratio_p95", "double"),
                SchemaField(f"{metric}_iqr_dev", "double"),
            ]
        )
    base_suspicion_fields = [
        SchemaField("anomaly_strength_core", "double"),
        SchemaField("beacon_suspicion_score", "double"),
        SchemaField("exfiltration_suspicion_score", "double"),
        SchemaField("lateral_movement_score", "double"),
        SchemaField("rare_pair_flag", "int"),
        SchemaField("beaconing_cadence_score", "double"),
        SchemaField("exfiltration_indicator", "double"),
        SchemaField("lateral_movement_indicator", "double"),
    ]
    if include_time_band_violation:
        base_suspicion_fields.insert(0, SchemaField("time_band_violation_flag", "int"))
    suspicion_fields.extend(base_suspicion_fields)

    rare_metrics = list(rare_pair_metrics or [])
    for metric in rare_metrics:
        suspicion_fields.append(SchemaField(f"rare_pair_weighted_z_{metric}", "double"))

    pair_fields = []
    for name in PAIR_RARITY_FIELDS:
        if name == "pair_last_seen_ts":
            pair_fields.append(SchemaField(name, "timestamp"))
        elif name in {"pair_seen_count", "active_days", "is_new_pair_flag", "is_rare_pair_flag"}:
            pair_fields.append(SchemaField(name, "long"))
        else:
            pair_fields.append(SchemaField(name, "double"))

    meta_fields = [
        SchemaField("host_ip", "string", False),
        SchemaField("window_label", "string", False),
        SchemaField("window_start_ts", "timestamp", False),
        SchemaField("window_end_ts", "timestamp", False),
        SchemaField("baseline_horizon", "string", False),
        SchemaField("baseline_start_ts", "timestamp"),
        SchemaField("baseline_end_ts", "timestamp"),
        SchemaField("record_id", "string", False),
        SchemaField("mini_batch_id", "string", False),
        SchemaField("feature_spec_version", "string", False),
        SchemaField("dt", "date", False),
    ]
    return SchemaManifest(
        name="fg_c",
        fields=meta_fields + correlation_fields + pair_fields + suspicion_fields,
    )


def build_fg_c_default_metrics() -> List[str]:
    """Execute the build fg c default metrics stage of the workflow."""
    return build_default_metric_list()

