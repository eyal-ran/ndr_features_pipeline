
"""FG-A schema utilities for NDR pipeline.

This module defines constants and helper functions for the FG-A (current
behaviour) feature group, including standard window definitions and
naming helpers. It does *not* enumerate every feature column explicitly;
instead, it encodes the conventions used by fg_a_builder_job to generate
column names programmatically.

FG-A rows are keyed by:
    - host_ip
    - window_label (e.g. "w_15m", "w_30m", "w_1h", "w_8h", "w_24h")
    - window_end_ts

Each row represents the behaviour of a single host in a specific role
(outbound and/or inbound) over a given lookback window ending at
window_end_ts. Features are prefixed by an optional direction prefix:
    - outbound (no prefix, e.g. sessions_cnt_w_15m)
    - inbound (prefix "in_", e.g. in_sessions_cnt_w_15m)

The full feature list is documented in the FG-A specification document.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


# Window configuration ---------------------------------------------------------------------


FG_A_WINDOWS_MINUTES: Dict[str, int] = {
    "w_15m": 15,
    "w_30m": 30,
    "w_1h": 60,
    "w_8h": 8 * 60,
    "w_24h": 24 * 60,
}


WINDOW_LABELS: List[str] = list(FG_A_WINDOWS_MINUTES.keys())


# Metric families --------------------------------------------------------------------------


OUTBOUND_BASE_METRICS: List[str] = [
    "sessions_cnt",
    "allow_cnt",
    "deny_cnt",
    "alert_cnt",
    "rst_cnt",
    "aged_out_cnt",
    "zero_reply_cnt",
    "short_session_cnt",
    "bytes_src_sum",
    "bytes_dst_sum",
    "duration_sum",
    # breadth / distribution
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
    # transport and threat
    "tcp_cnt",
    "udp_cnt",
    "icmp_cnt",
    "threat_cnt",
    "traffic_cnt",
    # port categories
    "high_risk_port_sessions_cnt",
    "admin_port_sessions_cnt",
    "fileshare_port_sessions_cnt",
    "directory_port_sessions_cnt",
    "db_port_sessions_cnt",
]


INBOUND_BASE_METRICS: List[str] = OUTBOUND_BASE_METRICS[:]  # same semantics, different role


DERIVED_METRICS: List[str] = [
    "duration_mean",
    "deny_ratio",
    "bytes_asymmetry_ratio",
    "transport_tcp_ratio",
    "transport_udp_ratio",
    "transport_icmp_ratio",
    "has_high_risk_port_activity",
]


NOVELTY_METRICS: List[str] = [
    "new_peer_cnt_lookback30d",
    "rare_peer_cnt_lookback30d",
    "new_peer_ratio_lookback30d",
    "new_dst_port_cnt_lookback30d",
    "rare_dst_port_cnt_lookback30d",
    "new_pair_cnt_lookback30d",
    "new_src_dst_port_cnt_lookback30d",
]


HIGH_RISK_SEGMENT_METRICS: List[str] = [
    "high_risk_segment_sessions_cnt",
    "high_risk_segment_unique_dsts",
    "has_high_risk_segment_interaction",
]


@dataclass(frozen=True)
class FGAMeta:
    """Metadata needed by fg_a_builder_job.

    Attributes
    ----------
    feature_spec_version:
        Version string for this FG-A spec (e.g. "fga_v1").
    feature_group_name_offline:
        SageMaker Feature Store offline feature group name.
    feature_group_name_online:
        SageMaker Feature Store online feature group name (optional).
    """

    feature_spec_version: str
    feature_group_name_offline: str
    feature_group_name_online: str | None = None


def build_feature_name(prefix: str, base_metric: str, window_label: str) -> str:
    """Return canonical FG-A feature column name.

    Parameters
    ----------
    prefix:
        Directional prefix. For outbound use "" (empty string),
        for inbound use "in_".
    base_metric:
        Base metric name from OUTBOUND_BASE_METRICS / INBOUND_BASE_METRICS
        or DERIVED_METRICS.
    window_label:
        Window label such as "w_15m".

    Examples
    --------
    >>> build_feature_name("", "sessions_cnt", "w_15m")
    'sessions_cnt_w_15m'
    >>> build_feature_name("in_", "sessions_cnt", "w_30m")
    'in_sessions_cnt_w_30m'
    """
    return f"{prefix}{base_metric}_{window_label}"
