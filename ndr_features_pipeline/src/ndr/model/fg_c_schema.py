"""FG-C schema defaults and helpers."""

from __future__ import annotations

from typing import List

from ndr.model.fg_a_schema import WINDOW_LABELS


DEFAULT_TRANSFORMS: List[str] = ["z_mad", "ratio", "log_ratio"]


DEFAULT_BASE_METRICS: List[str] = [
    "sessions_cnt",
    "allow_cnt",
    "deny_cnt",
    "alert_cnt",
    "rst_cnt",
    "zero_reply_cnt",
    "short_session_cnt",
    "bytes_src_sum",
    "bytes_dst_sum",
    "duration_sum",
    "peer_ip_nunique",
    "peer_port_nunique",
    "peer_segment_nunique",
    "peer_ip_entropy",
    "peer_port_entropy",
    "peer_ip_top1_sessions_share",
    "peer_port_top1_sessions_share",
    "peer_ip_top1_bytes_share",
    "peer_port_top1_bytes_share",
    "max_sessions_per_minute",
    "high_risk_port_sessions_cnt",
    "admin_port_sessions_cnt",
    "fileshare_port_sessions_cnt",
    "directory_port_sessions_cnt",
    "db_port_sessions_cnt",
    "traffic_cnt",
    "threat_cnt",
]


DEFAULT_DERIVED_METRICS: List[str] = [
    "duration_mean",
    "deny_ratio",
    "bytes_asymmetry_ratio",
    "transport_tcp_ratio",
    "transport_udp_ratio",
    "transport_icmp_ratio",
    "has_high_risk_port_activity",
    "new_peer_cnt_lookback30d",
    "rare_peer_cnt_lookback30d",
    "new_peer_ratio_lookback30d",
    "new_dst_port_cnt_lookback30d",
    "rare_dst_port_cnt_lookback30d",
    "new_pair_cnt_lookback30d",
    "new_src_dst_port_cnt_lookback30d",
    "high_risk_segment_sessions_cnt",
    "high_risk_segment_unique_dsts",
    "has_high_risk_segment_interaction",
]


def build_default_metric_list(window_labels: List[str] | None = None) -> List[str]:
    """Return the curated FG-C metric list with window/direction expansion."""
    labels = window_labels or WINDOW_LABELS
    prefixes = ["", "in_"]
    metrics: List[str] = []
    for prefix in prefixes:
        for base_metric in DEFAULT_BASE_METRICS + DEFAULT_DERIVED_METRICS:
            for window_label in labels:
                metrics.append(f"{prefix}{base_metric}_{window_label}")
    return metrics

