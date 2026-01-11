import types

from ndr.config.job_spec_loader import JobSpecLoader
from ndr.config.job_spec_models import JobSpec


class DummyTable:
    def __init__(self, item):
        self._item = item

    def get_item(self, Key):
        return {"Item": self._item}


class DummyDDBResource:
    def __init__(self, item):
        self._item = item

    def Table(self, name):
        return DummyTable(self._item)


def test_job_spec_loader_from_dict(monkeypatch):
    """Basic test for JobSpecLoader._from_dict mapping."""
    payload = {
        "project_name": "proj1",
        "job_name": "delta_builder",
        "feature_spec_version": "v1",
        "input": {"s3_prefix": "s3://bucket/prefix", "format": "json", "compression": "gzip"},
        "dq": {},
        "enrichment": {},
        "roles": [
            {
                "name": "outbound",
                "host_ip": "source_ip",
                "peer_ip": "destination_ip",
                "bytes_sent": "source_bytes",
                "bytes_recv": "destination_bytes",
                "peer_port": "destination_port",
            }
        ],
        "operators": [],
        "output": {"s3_prefix": "s3://out/prefix", "format": "parquet", "partition_keys": ["dt"]},
    }
    item = {"spec": payload}

    def fake_resource(name):
        assert name == "dynamodb"
        return DummyDDBResource(item)

    monkeypatch.setattr("boto3.resource", fake_resource)

    loader = JobSpecLoader(table_name="dummy-table")
    spec = loader.load(project_name="proj1", job_name="delta_builder")
    assert isinstance(spec, JobSpec)
    assert spec.project_name == "proj1"
    assert spec.output.s3_prefix == "s3://out/prefix"
