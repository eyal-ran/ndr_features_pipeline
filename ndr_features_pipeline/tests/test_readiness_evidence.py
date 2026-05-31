from types import SimpleNamespace
from ndr.processing.readiness_evidence import (
    RtReadinessEvidenceEvaluator,
    MonthlyReadinessEvidenceEvaluator,
)


class Probe:
    def __init__(self, missing=()):
        self.missing = set(missing)

    def exists(self, uri):
        return uri not in self.missing


class Loader:
    def __init__(self, record=None, records=()):
        self.record = record
        self.records = list(records)

    def get_batch(self, **_):
        return self.record

    def lookup_forward(self, **_):
        return self.records


def record(etl="2026-04-01T00:00:00Z"):
    return SimpleNamespace(
        etl_ts=etl,
        s3_prefixes={
            "dpp": {
                "fg_a": "s3://b/a",
                "pair_counts": "s3://b/p",
                "fg_b": {"machines_manifest": "s3://b/f"},
            },
            "mlp": {"mlp": {}},
        },
    )


def test_rt_evidence_reports_only_missing_objects():
    evidence = RtReadinessEvidenceEvaluator(
        batch_index_loader=Loader(record()), artifact_probe=Probe({"s3://b/p"})
    ).evaluate(
        project_name="p",
        ml_project_name="mlp",
        mini_batch_id="b",
        batch_start_ts_iso="2026-04-01T00:00:00Z",
        batch_end_ts_iso="2026-04-01T00:15:00Z",
    )
    assert evidence.required_families == ["fg_a", "pair_counts", "fg_b_baseline"]
    assert [r["family"] for r in evidence.missing_ranges] == ["pair_counts"]


def test_monthly_evidence_coalesces_adjacent_ranges():
    evidence = MonthlyReadinessEvidenceEvaluator(
        batch_index_loader=Loader(records=[record(), record("2026-04-01T00:15:00Z")]),
        artifact_probe=Probe({"s3://b/a"}),
    ).evaluate(project_name="p", reference_month="2026/04")
    fg_a = [r for r in evidence.missing_ranges if r["family"] == "fg_a"]
    assert fg_a == [
        {
            "family": "fg_a",
            "start_ts_iso": "2026-04-01T00:00:00Z",
            "end_ts_iso": "2026-04-01T00:30:00Z",
            "reason_code": "artifact_object_missing",
        }
    ]
