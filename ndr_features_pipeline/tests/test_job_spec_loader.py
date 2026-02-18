import sys
import types

boto3_stub = types.ModuleType("boto3")
boto3_stub.resource = lambda *_args, **_kwargs: None
sys.modules["boto3"] = boto3_stub

from ndr.config.job_spec_loader import JobSpecLoader, load_pair_counts_job_spec
from ndr.config.job_spec_models import JobSpec, PairCountsJobSpec


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

    import ndr.config.job_spec_loader as loader_module
    monkeypatch.setattr(loader_module.boto3, "resource", fake_resource, raising=False)

    loader = JobSpecLoader(table_name="dummy-table")
    spec = loader.load(project_name="proj1", job_name="delta_builder")
    assert isinstance(spec, JobSpec)
    assert spec.project_name == "proj1"
    assert spec.output.s3_prefix == "s3://out/prefix"


def test_load_pair_counts_job_spec_success(monkeypatch):
    payload = {
        "traffic_input": {
            "s3_prefix": "s3://bucket/traffic/",
            "layout": "batch_folder",
            "field_mapping": {
                "source_ip": "src",
                "destination_ip": "dst",
                "destination_port": "dport",
                "event_start": "event_start",
                "event_end": "event_end",
            },
        },
        "pair_counts_output": {"s3_prefix": "s3://bucket/pair_counts/"},
    }
    item = {"spec": payload}

    def fake_resource(name):
        assert name == "dynamodb"
        return DummyDDBResource(item)

    import ndr.config.job_spec_loader as loader_module
    monkeypatch.setattr(loader_module.boto3, "resource", fake_resource, raising=False)

    spec = load_pair_counts_job_spec(
        project_name="proj1",
        feature_spec_version="v1",
        table_name="dummy-table",
    )
    assert isinstance(spec, PairCountsJobSpec)
    assert spec.traffic_input.field_mapping["source_ip"] == "src"


def test_load_pair_counts_job_spec_requires_mapping(monkeypatch):
    payload = {
        "traffic_input": {
            "s3_prefix": "s3://bucket/traffic/",
            "layout": "batch_folder",
        },
        "pair_counts_output": {"s3_prefix": "s3://bucket/pair_counts/"},
    }
    item = {"spec": payload}

    def fake_resource(name):
        assert name == "dynamodb"
        return DummyDDBResource(item)

    import ndr.config.job_spec_loader as loader_module
    monkeypatch.setattr(loader_module.boto3, "resource", fake_resource, raising=False)

    try:
        load_pair_counts_job_spec(
            project_name="proj1",
            feature_spec_version="v1",
            table_name="dummy-table",
        )
        raise AssertionError("Expected ValueError for missing mapping")
    except ValueError as exc:
        assert "field_mapping" in str(exc)
