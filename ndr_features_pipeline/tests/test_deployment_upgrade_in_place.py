import hashlib
import json
import re
import time
from pathlib import Path

import pytest


DEPLOYMENT_PLAN_PATH = Path("src/ndr/deployment/canonical_end_to_end_deployment_plan.md")


class _ClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


def _deployment_helpers() -> dict:
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"```python\n(?P<body>def _find_unresolved_placeholder\(.*?\n)```",
        deployment_plan,
        re.DOTALL,
    )
    assert match, "Deployment helper cell not found"
    namespace = {"ClientError": _ClientError, "hashlib": hashlib, "json": json, "time": time}
    exec(match.group("body"), namespace)
    return namespace




def _step_function_deployment_helpers() -> dict:
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"(?P<body>def _event_bus_kwargs\(.*?)(?=\neventbridge_rules =)",
        deployment_plan,
        re.DOTALL,
    )
    assert match, "Step Functions deployment helper block not found"
    namespace = {"ClientError": _ClientError, "json": json, "time": time}
    exec(match.group("body"), namespace)
    return namespace


def _contract():
    return {
        "table_name": "config",
        "billing_mode": "PAY_PER_REQUEST",
        "attribute_definitions": [
            {"AttributeName": "project_name", "AttributeType": "S"},
            {"AttributeName": "job_name_version", "AttributeType": "S"},
        ],
        "key_schema": [
            {"AttributeName": "project_name", "KeyType": "HASH"},
            {"AttributeName": "job_name_version", "KeyType": "RANGE"},
        ],
    }


def test_missing_table_is_created_during_reconciliation():
    helpers = _deployment_helpers()

    class _Waiter:
        def __init__(self):
            self.calls = []

        def wait(self, **kwargs):
            self.calls.append(kwargs)

    class _Client:
        def __init__(self):
            self.created = []
            self.waiter = _Waiter()

        def describe_table(self, **_kwargs):
            raise _ClientError("ResourceNotFoundException")

        def create_table(self, **kwargs):
            self.created.append(kwargs)

        def get_waiter(self, name):
            assert name == "table_exists"
            return self.waiter

    client = _Client()
    contract = _contract()

    helpers["ensure_ddb_table"](client, contract, dry_run=False)

    assert client.created == [{
        "TableName": "config",
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": contract["key_schema"],
        "AttributeDefinitions": contract["attribute_definitions"],
    }]
    assert client.waiter.calls == [{"TableName": "config"}]


def test_existing_compatible_table_is_preserved():
    helpers = _deployment_helpers()
    contract = _contract()

    class _Client:
        def describe_table(self, **_kwargs):
            return {
                "Table": {
                    "AttributeDefinitions": contract["attribute_definitions"],
                    "KeySchema": contract["key_schema"],
                }
            }

        def create_table(self, **_kwargs):
            raise AssertionError("compatible table must not be recreated")

    helpers["ensure_ddb_table"](_Client(), contract, dry_run=False)


def test_reconciliation_upserts_deployment_owned_pipeline_job_specs():
    helpers = _deployment_helpers()

    class _Table:
        def __init__(self):
            self.calls = []

        def put_item(self, **kwargs):
            self.calls.append(kwargs)

    table = _Table()
    resource = type("Resource", (), {"Table": lambda _self, _name: table})()
    item = {"project_name": "ndr", "job_name_version": "pipeline_rt_readiness_v1"}

    helpers["seed_table_items"](resource, "dpp_config", _contract(), [item], dry_run=False)

    assert table.calls == [{"Item": item}]


def test_reconciliation_inserts_environment_owned_seed_only_when_absent():
    helpers = _deployment_helpers()

    class _Table:
        def __init__(self):
            self.calls = []

        def put_item(self, **kwargs):
            self.calls.append(kwargs)

    table = _Table()
    resource = type("Resource", (), {"Table": lambda _self, _name: table})()
    item = {"project_name": "ndr", "job_name_version": "S3_ROOTS"}

    helpers["seed_table_items"](resource, "dpp_config", _contract(), [item], dry_run=False)

    assert table.calls == [{
        "Item": item,
        "ConditionExpression": "attribute_not_exists(#key0) AND attribute_not_exists(#key1)",
        "ExpressionAttributeNames": {"#key0": "project_name", "#key1": "job_name_version"},
    }]


def test_reconciliation_preserves_existing_environment_owned_seed():
    helpers = _deployment_helpers()

    class _Table:
        def put_item(self, **_kwargs):
            raise _ClientError("ConditionalCheckFailedException")

    resource = type("Resource", (), {"Table": lambda _self, _name: _Table()})()
    item = {"project_name": "ndr", "job_name_version": "S3_ROOTS"}

    helpers["seed_table_items"](resource, "dpp_config", _contract(), [item], dry_run=False)


def test_existing_table_schema_mismatch_fails_fast():
    helpers = _deployment_helpers()
    existing = _contract()
    existing["KeySchema"] = [{"AttributeName": "project_name", "KeyType": "HASH"}]
    existing["AttributeDefinitions"] = _contract()["attribute_definitions"]

    with pytest.raises(ValueError, match="incompatible key schema"):
        helpers["_validate_existing_ddb_table"](existing, _contract())


def test_deployment_notebook_uses_one_reconciliation_path():
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")

    assert "DEPLOYMENT_MODE" not in deployment_plan
    assert "build_deployment_seed_plan" not in deployment_plan
    assert "deploy_ddb(ddb_table_contracts, ddb_seed_items_plan" in deployment_plan


def _marker(**overrides):
    helpers = _deployment_helpers()
    kwargs = {
        "project_name": "ndr",
        "environment_name": "dev",
        "feature_spec_version": "v3",
        "deployment_revision": 1,
        "artifact_build_id": "build-1",
        "artifact_sha256": "abc123",
        "code_artifact_s3_uri": "s3://bucket/artifacts/code/build-1/source.tar.gz",
        "table_contracts": {"dpp_config": _contract()},
        "s3_schema": {"bucket_name": "bucket", "prefixes": ["artifacts/code"]},
        "pipeline_names": ["pipeline_rt_readiness"],
        "pipeline_definitions": {"ndr-v3-pipeline_rt_readiness": "{}"},
        "pipeline_job_specs": [{"updated_at": "ignored", "job_name_version": "pipeline_rt_readiness#v3"}],
        "rendered_step_functions": {"rt.json": "{}"},
        "eventbridge_config": {"event_bus_name": "ndr-dev-events", "rules": []},
        "applied_at": "2026-05-31T00:00:00Z",
    }
    kwargs.update(overrides)
    return helpers["build_deployment_marker"](**kwargs)


def test_deployment_marker_is_stable_across_unchanged_reruns():
    first = _marker(applied_at="2026-05-31T00:00:00Z")
    second = _marker(applied_at="2026-06-01T00:00:00Z", pipeline_job_specs=[{"updated_at": "also-ignored", "job_name_version": "pipeline_rt_readiness#v3"}])

    assert first["deployment_fingerprint"] == second["deployment_fingerprint"]


def test_deployment_revision_forces_a_new_fingerprint():
    assert _marker(deployment_revision=1)["deployment_fingerprint"] != _marker(deployment_revision=2)["deployment_fingerprint"]


def test_legacy_deployment_without_marker_is_adopted_after_reconciliation():
    helpers = _deployment_helpers()

    class _Table:
        def __init__(self):
            self.put_calls = []

        def get_item(self, **_kwargs):
            return {}

        def put_item(self, **kwargs):
            self.put_calls.append(kwargs)

    table = _Table()
    resource = type("Resource", (), {"Table": lambda _self, _name: table})()
    marker = _marker()

    result = helpers["persist_deployment_marker"](resource, "config", marker, dry_run=False)

    assert result["action"] == "created"
    assert table.put_calls == [{"Item": marker}]


def test_marker_is_persisted_after_structural_checks_in_notebook():
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")

    step_functions = deployment_plan.index("step_function_results = upsert_step_functions")
    checks = deployment_plan.index("print('Structural readiness checks passed.')")
    persist = deployment_plan.index("deployment_marker_result = persist_deployment_marker")
    assert step_functions < checks < persist
    assert "deployment_revision = 1" in deployment_plan


def test_existing_matching_marker_is_reported_unchanged():
    helpers = _deployment_helpers()
    marker = _marker()

    class _Table:
        def __init__(self):
            self.put_calls = []

        def get_item(self, **_kwargs):
            return {"Item": marker}

        def put_item(self, **kwargs):
            self.put_calls.append(kwargs)

    table = _Table()
    resource = type("Resource", (), {"Table": lambda _self, _name: table})()

    result = helpers["persist_deployment_marker"](resource, "config", marker, dry_run=False)

    assert result["action"] == "unchanged"
    assert result["previous_fingerprint"] == marker["deployment_fingerprint"]
    assert table.put_calls == [{"Item": marker}]


def test_notebook_has_executable_completion_flow_without_manual_sfn_placeholders():
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")

    assert "subprocess.run(command, check=True)" in deployment_plan
    assert "reconcile_event_bus(" in deployment_plan
    assert "reconcile_eventbridge_rules(" in deployment_plan
    assert "sfn_client.start_execution(" in deployment_plan
    assert "<REPLACE_SFN" not in deployment_plan
    assert "Trigger pipeline starts here" not in deployment_plan


def test_preflight_rejects_unresolved_workload_placeholders():
    helpers = _deployment_helpers()

    with pytest.raises(ValueError, match="Unresolved deployment placeholder"):
        helpers["validate_deployment_preflight"]({"project_name": "ndr"}, {"dpp_config": [{"spec": {"model": "REPLACE_ME"}}]})


def test_workload_job_specs_are_deployment_owned_for_legacy_repairs():
    helpers = _deployment_helpers()

    assert helpers["_is_deployment_owned_seed"]("dpp_config", {"job_name_version": "inference_predictions#v3", "spec": {}})
    assert not helpers["_is_deployment_owned_seed"]("dpp_config", {"job_name_version": "project_parameters#v3", "spec": {}})


def test_bootstrap_startup_waits_for_success_before_returning():
    helpers = _step_function_deployment_helpers()

    class _SfnClient:
        def start_execution(self, **_kwargs):
            return {"executionArn": "arn:execution:bootstrap"}

        def describe_execution(self, **_kwargs):
            return {"status": "SUCCEEDED"}

    result = helpers["start_initial_materialization_runs"](
        _SfnClient(), "arn:state-machine:bootstrap", "bootstrap-dev-v3-r1", {}, dry_run=False
    )

    assert result == {"action": "started", "execution_arn": "arn:execution:bootstrap", "status": "SUCCEEDED"}


def test_monthly_schedule_does_not_freeze_deployment_time_reference_month():
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")
    monthly = Path("docs/step_functions_jsonata/sfn_ndr_monthly_fg_b_baselines.json").read_text(encoding="utf-8")

    monthly_rule = next(line for line in deployment_plan.splitlines() if "'schedule-monthly'" in line and "'schedule_expression'" in line)
    assert "'reference_month': reference_month" not in monthly_rule
    assert "'event_bus_name': event_bus_name" not in monthly_rule
    assert "$substring($states.context.Execution.StartTime,0,7)" in monthly


def test_15m_trigger_forwards_canonical_ingestion_event_detail():
    deployment_plan = DEPLOYMENT_PLAN_PATH.read_text(encoding="utf-8")

    trigger = next(line for line in deployment_plan.splitlines() if "'trigger-15m-ingestion'" in line)
    assert "'schedule_expression'" not in trigger
    assert "'event_pattern'" in trigger
    assert "'source': ['ndr.ingestion']" in trigger
    assert "'detail-type': ['NdrRawParsedLogsBatchCompleted']" in trigger
    assert "'input_path': '$.detail'" in trigger
    for field in ("project_name", "data_source_name", "batch_id", "raw_parsed_logs_s3_prefix", "timestamp", "feature_spec_version"):
        assert f"'{field}': [{{'exists': True}}]" in trigger


def test_eventbridge_legacy_rule_cleanup_is_replay_safe():
    helpers = _step_function_deployment_helpers()

    class _EventsClient:
        def __init__(self):
            self.removed = []
            self.deleted = []

        def list_targets_by_rule(self, **kwargs):
            return {"Targets": [{"Id": "legacy-target"}]}

        def remove_targets(self, **kwargs):
            self.removed.append(kwargs)

        def delete_rule(self, **kwargs):
            self.deleted.append(kwargs)

    client = _EventsClient()
    helpers["remove_eventbridge_rules"](
        client,
        [{"name": "legacy", "event_bus_name": "ndr"}],
        dry_run=False,
    )

    assert client.removed == [{"Rule": "legacy", "Ids": ["legacy-target"], "EventBusName": "ndr"}]
    assert client.deleted == [{"Name": "legacy", "EventBusName": "ndr"}]


def test_eventbridge_reconciliation_supports_event_rules_and_default_bus_schedules():
    helpers = _step_function_deployment_helpers()

    class _EventsClient:
        def __init__(self):
            self.rules = []
            self.targets = []

        def put_rule(self, **kwargs):
            self.rules.append(kwargs)

        def put_targets(self, **kwargs):
            self.targets.append(kwargs)

    client = _EventsClient()
    helpers["reconcile_eventbridge_rules"](
        client,
        [
            {"name": "rt", "event_bus_name": "ndr", "event_pattern": {"source": ["ndr.ingestion"]}, "target_arn": "arn:rt", "role_arn": "arn:role", "input_path": "$.detail"},
            {"name": "monthly", "schedule_expression": "cron(0 0 1 * ? *)", "target_arn": "arn:monthly", "role_arn": "arn:role", "input": {"project_name": "ndr"}},
        ],
        dry_run=False,
    )

    assert client.rules == [
        {"Name": "rt", "State": "ENABLED", "EventBusName": "ndr", "EventPattern": '{"source": ["ndr.ingestion"]}'},
        {"Name": "monthly", "State": "ENABLED", "ScheduleExpression": "cron(0 0 1 * ? *)"},
    ]
    assert client.targets == [
        {"Rule": "rt", "EventBusName": "ndr", "Targets": [{"Id": "rt", "Arn": "arn:rt", "RoleArn": "arn:role", "InputPath": "$.detail"}]},
        {"Rule": "monthly", "Targets": [{"Id": "monthly", "Arn": "arn:monthly", "RoleArn": "arn:role", "Input": '{"project_name": "ndr"}'}]},
    ]


def test_failed_existing_bootstrap_execution_is_retried(monkeypatch):
    helpers = _step_function_deployment_helpers()
    monkeypatch.setattr(time, "time", lambda: 1234567890)

    class _SfnClient:
        def __init__(self):
            self.starts = []

        def start_execution(self, **kwargs):
            self.starts.append(kwargs)
            if len(self.starts) == 1:
                raise _ClientError("ExecutionAlreadyExists")
            return {"executionArn": "arn:execution:retry"}

        def list_executions(self, **_kwargs):
            return {"executions": [{"name": "bootstrap-dev-v3-r1", "executionArn": "arn:execution:failed"}]}

        def describe_execution(self, executionArn):
            return {"status": "FAILED" if executionArn.endswith("failed") else "SUCCEEDED"}

    client = _SfnClient()
    result = helpers["start_initial_materialization_runs"](
        client, "arn:state-machine:bootstrap", "bootstrap-dev-v3-r1", {}, dry_run=False
    )

    assert result == {"action": "retried", "execution_arn": "arn:execution:retry", "status": "SUCCEEDED"}
    assert client.starts[1]["name"] == "bootstrap-dev-v3-r1-retry-1234567890"
