"""NDR job spec models module."""
from dataclasses import dataclass, field

from typing import Any, Dict, List, Optional


@dataclass
class InputSpec:
    """Configuration for reading input data from S3 into Spark."""
    s3_prefix: str
    format: str
    compression: Optional[str] = None
    schema_projection: Optional[List[str]] = None
    field_mapping: Optional[Dict[str, str]] = None


@dataclass
class DQSpec:
    """Data-quality and cleaning rules to apply before aggregation."""
    drop_malformed_ip: bool = True
    duration_non_negative: bool = True
    bytes_non_negative: bool = True
    filter_null_bytes_ports: bool = True
    emit_metrics: bool = True


@dataclass
class EnrichmentSpec:
    """Additional enrichment sources, e.g. VDI prefixes and port sets."""
    vdi_hostname_prefixes: List[str] = field(default_factory=list)
    port_sets_location: Optional[str] = None  # e.g., s3://bucket/config/port_sets.json


@dataclass
class RoleMappingSpec:
    """Defines how to map raw columns into role-neutral host/peer columns."""
    name: str                  # "outbound" or "inbound"
    host_ip: str               # e.g., "source_ip" or "destination_ip"
    peer_ip: str               # e.g., "destination_ip" or "source_ip"
    bytes_sent: str            # column name
    bytes_recv: str            # column name
    peer_port: str             # column name


@dataclass
class OperatorSpec:
    """Specifies a single operator to apply (delta_builder_operators.*)."""
    type: str                  # "base_counts_and_sums", "quantiles", etc.
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OutputSpec:
    """Output dataset configuration for the job."""
    s3_prefix: str
    format: str
    partition_keys: List[str]
    write_mode: str = "overwrite"


@dataclass
class JobSpec:
    """Top-level job configuration loaded from DynamoDB."""
    project_name: str
    job_name: str
    feature_spec_version: str
    input: InputSpec
    dq: DQSpec
    enrichment: EnrichmentSpec
    roles: List[RoleMappingSpec]
    operators: List[OperatorSpec]
    output: OutputSpec
    pair_context_output: Optional[OutputSpec] = None


@dataclass
class PairCountsTrafficInputSpec:
    """Input configuration for Pair-Counts traffic reads."""

    s3_prefix: str
    layout: str = "batch_folder"
    field_mapping: Dict[str, str] = field(default_factory=dict)


@dataclass
class PairCountsFilterSpec:
    """Optional filters for Pair-Counts source rows."""

    require_nonnull_ips: bool = True
    require_destination_port: bool = True


@dataclass
class PairCountsOutputSpec:
    """Output configuration for Pair-Counts dataset."""

    s3_prefix: str


@dataclass
class PairCountsJobSpec:
    """Strongly typed JobSpec payload for ``pair_counts_builder``."""

    traffic_input: PairCountsTrafficInputSpec
    pair_counts_output: PairCountsOutputSpec
    filters: PairCountsFilterSpec = field(default_factory=PairCountsFilterSpec)
    segment_mapping: Dict[str, Any] = field(default_factory=dict)
