from ndr.config.job_spec_models import JobSpec, InputSpec, DQSpec, EnrichmentSpec, RoleMappingSpec, OperatorSpec, OutputSpec


def test_job_spec_smoke():
    """Very lightweight smoke test for JobSpec construction."""
    JobSpec(
        project_name="proj1",
        job_name="delta_builder",
        feature_spec_version="v1",
        input=InputSpec(s3_prefix="s3://bucket/prefix", format="json"),
        dq=DQSpec(),
        enrichment=EnrichmentSpec(),
        roles=[
            RoleMappingSpec(
                name="outbound",
                host_ip="source_ip",
                peer_ip="destination_ip",
                bytes_sent="source_bytes",
                bytes_recv="destination_bytes",
                peer_port="destination_port",
            )
        ],
        operators=[OperatorSpec(type="base_counts_and_sums")],
        output=OutputSpec(s3_prefix="s3://out/prefix", format="parquet", partition_keys=["dt"]),
    )
