from ndr.pipeline.io_contract import resolve_step_code_uri, resolve_step_script_contract


def test_resolve_step_script_contract_success():
    contract = resolve_step_script_contract(
        {
            "scripts": {
                "steps": {
                    "DeltaBuilderStep": {
                        "code_prefix_s3": "s3://bucket/prefix/",
                        "entry_script": "run_delta_builder.py",
                    }
                }
            }
        },
        step_name="DeltaBuilderStep",
    )

    assert contract.script_s3_uri == "s3://bucket/prefix/run_delta_builder.py"


def test_resolve_step_script_contract_missing_step_raises():
    try:
        resolve_step_script_contract({"scripts": {"steps": {}}}, step_name="Missing")
    except ValueError as exc:
        assert "scripts.steps.Missing" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_resolve_step_code_uri_loads_pipeline_spec(monkeypatch):
    def _fake_load_job_spec(project_name, job_name, feature_spec_version, table_name=None):
        assert project_name == "ndr-project"
        assert job_name == "pipeline_15m_streaming"
        assert feature_spec_version == "v1"
        assert table_name is None
        return {
            "scripts": {
                "steps": {
                    "DeltaBuilderStep": {
                        "code_prefix_s3": "s3://bucket/path",
                        "entry_script": "run_delta_builder.py",
                    }
                }
            }
        }

    monkeypatch.setattr("ndr.pipeline.io_contract.load_job_spec", _fake_load_job_spec)

    uri = resolve_step_code_uri(
        project_name="ndr-project",
        feature_spec_version="v1",
        pipeline_job_name="pipeline_15m_streaming",
        step_name="DeltaBuilderStep",
    )

    assert uri == "s3://bucket/path/run_delta_builder.py"
