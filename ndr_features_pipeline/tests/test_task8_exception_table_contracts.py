import json
from pathlib import Path

import pytest
import sys
import types

_boto3 = types.ModuleType("boto3")
_boto3.client = lambda *_args, **_kwargs: None
sys.modules.setdefault("boto3", _boto3)

from ndr.config.exception_table_contracts import (
    EXCEPTION_TABLE_CONTRACTS,
    ExceptionTableContractError,
    assert_operation_allowed,
    classify_ddb_error,
    validate_table_schema,
)
from ndr.scripts.run_exception_table_preflight import main as run_preflight_main


STEP_FUNCTIONS_DIR = Path(__file__).resolve().parents[1] / "docs" / "step_functions_jsonata"


def _load_state_machine(name: str) -> dict:
    return json.loads((STEP_FUNCTIONS_DIR / name).read_text(encoding="utf-8"))


def _collect_states(state_machine: dict) -> dict:
    collected = {}

    def _visit(states: dict):
        for key, value in states.items():
            collected[key] = value
            if isinstance(value, dict) and value.get("Type") == "Map" and "ItemProcessor" in value:
                _visit(value["ItemProcessor"]["States"])

    _visit(state_machine["States"])
    return collected


def test_contract_registry_contains_all_exception_tables():
    assert set(EXCEPTION_TABLE_CONTRACTS) == {"routing", "processing_lock", "publication_lock"}


def test_operation_forbidden_fails_fast():
    with pytest.raises(ExceptionTableContractError) as exc:
        assert_operation_allowed(logical_name="routing", operation="DeleteItem")
    assert exc.value.code == "EXC_TABLE_OPERATION_FORBIDDEN"


def test_schema_drift_fails_fast_with_explicit_error_code():
    with pytest.raises(ExceptionTableContractError) as exc:
        validate_table_schema(
            logical_name="processing_lock",
            table_name="processing_lock",
            table_description={"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]},
        )
    assert exc.value.code == "EXC_TABLE_SCHEMA_DRIFT"


def test_missing_table_is_non_retriable_contract_error():
    class _Missing(Exception):
        response = {"Error": {"Code": "ResourceNotFoundException"}}

    err = classify_ddb_error(logical_name="routing", table_name="ml_projects_routing", error=_Missing())
    assert err.code == "EXC_TABLE_MISSING"
    assert err.retriable is False


def test_access_denied_is_non_retriable_contract_error():
    class _Denied(Exception):
        response = {"Error": {"Code": "AccessDeniedException"}}

    err = classify_ddb_error(logical_name="routing", table_name="ml_projects_routing", error=_Denied())
    assert err.code == "EXC_TABLE_ACCESS_DENIED"
    assert err.retriable is False


def test_preflight_validates_required_tables_for_flow(monkeypatch):
    class _Client:
        def describe_table(self, TableName):
            if TableName == "processing_lock":
                return {"Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}]}}
            raise AssertionError(f"Unexpected table lookup: {TableName}")

    monkeypatch.setattr("ndr.scripts.run_exception_table_preflight.boto3.client", lambda *_args, **_kwargs: _Client())
    rc = run_preflight_main(["--flow", "monthly"])
    assert rc == 0


def test_sfn_table_operations_conform_to_registry_allowed_operations():
    inference = _load_state_machine("sfn_ndr_15m_features_inference.json")
    publication = _load_state_machine("sfn_ndr_prediction_publication.json")

    # routing table is used via getItem only in inference flow.
    routing_resource = inference["States"]["LoadProjectRoutingFromDynamo"]["Resource"].split(":")[-1]
    routing_ops = {"GetItem" if routing_resource == "getItem" else routing_resource}
    assert "GetItem" in EXCEPTION_TABLE_CONTRACTS["routing"].allowed_operations
    assert routing_ops == {"GetItem"}

    # processing lock table in 15m flow uses put/delete only.
    processing_ops = {
        inference["States"]["AcquireMiniBatchLock"]["Resource"].split(":")[-1].replace("putItem", "PutItem").replace("deleteItem", "DeleteItem"),
        inference["States"]["ReleaseMiniBatchLockOnSuccess"]["Resource"].split(":")[-1].replace("putItem", "PutItem").replace("deleteItem", "DeleteItem"),
    }
    assert processing_ops.issubset(set(EXCEPTION_TABLE_CONTRACTS["processing_lock"].allowed_operations))

    # publication lock flow uses put/update in publication orchestrator.
    publication_states = _collect_states(publication)
    ops = set()
    for state_name in ("AcquirePublicationLock", "MarkPublicationSucceeded", "MarkPublicationFailed"):
        op = publication_states[state_name]["Resource"].split(":")[-1]
        if op == "putItem":
            ops.add("PutItem")
        elif op == "updateItem":
            ops.add("UpdateItem")
    assert ops.issubset(set(EXCEPTION_TABLE_CONTRACTS["publication_lock"].allowed_operations))
