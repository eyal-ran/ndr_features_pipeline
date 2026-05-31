import json
import re
from pathlib import Path


def test_readiness_pipeline_seed_specs_are_deployable():
    text = Path("src/ndr/scripts/create_ml_projects_parameters_table.py").read_text()
    assert '_versioned_job_name("pipeline_rt_readiness", feature_spec_version)' in text
    assert '"RtArtifactReadinessCheckerStep"' in text
    assert '"entry_script": "run_rt_readiness_checker.py"' in text
    assert (
        '_versioned_job_name("pipeline_monthly_readiness", feature_spec_version)'
        in text
    )
    assert '"MonthlyFgBReadinessCheckerStep"' in text
    assert '"entry_script": "run_monthly_readiness_checker.py"' in text


def test_deployment_substitutions_use_dedicated_readiness_pipelines():
    text = Path(
        "src/ndr/deployment/canonical_end_to_end_deployment_plan.md"
    ).read_text()
    assert '"PipelineNameRtReadiness": _pipeline_name("pipeline_rt_readiness")' in text
    assert (
        '"PipelineNameMonthlyReadiness": _pipeline_name("pipeline_monthly_readiness")'
        in text
    )
    assert "'pipeline_rt_readiness': build_rt_readiness_pipeline" in text
    assert "'pipeline_monthly_readiness': build_monthly_readiness_pipeline" in text


def test_all_step_function_pipeline_placeholders_have_substitutions():
    deployment = Path(
        "src/ndr/deployment/canonical_end_to_end_deployment_plan.md"
    ).read_text()
    substitutions = set(re.findall(r'"(PipelineName[A-Za-z0-9_]+)":', deployment))
    placeholders = set()
    for path in Path("docs/step_functions_jsonata").glob("*.json"):
        placeholders.update(
            re.findall(r"\$\{(PipelineName[A-Za-z0-9_]+)\}", path.read_text())
        )
    assert placeholders <= substitutions


def test_monthly_recheck_polls_latest_execution():
    states = json.loads(
        Path(
            "docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json"
        ).read_text()
    )["States"]
    assert (
        "monthly_recheck_readiness_execution_arn"
        in states["DescribeMonthlyReadinessPipeline"]["Arguments"][
            "PipelineExecutionArn"
        ]
    )
