"""CI guard: fail on undeclared interface drift from frozen flow_contract_matrix_v2."""

from __future__ import annotations

import ast
import json
from pathlib import Path

from ndr.contracts_v2 import assert_contract_surface, load_contract_matrix

REPO_ROOT = Path(__file__).resolve().parents[3]


def _find_state(node: object, state_name: str) -> dict | None:
    if isinstance(node, dict):
        states = node.get("States")
        if isinstance(states, dict) and state_name in states:
            return states[state_name]
        for value in node.values():
            found = _find_state(value, state_name)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _find_state(value, state_name)
            if found is not None:
                return found
    return None


def _extract_pipeline_params(state_machine_path: Path, state_name: str) -> set[str]:
    doc = json.loads(state_machine_path.read_text(encoding="utf-8"))
    state = _find_state(doc, state_name)
    if not isinstance(state, dict):
        raise KeyError(f"state={state_name} not found in {state_machine_path}")
    params = state["Arguments"]["PipelineParameters"]
    return {str(entry["Name"]) for entry in params}


def _extract_cli_args(script_path: Path) -> set[str]:
    tree = ast.parse(script_path.read_text(encoding="utf-8"), filename=str(script_path))
    args: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "add_argument":
            continue
        if not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str) and first.value.startswith("--"):
            args.add(first.value)
    return args


def main() -> int:
    matrix = load_contract_matrix()

    sfn_dir = REPO_ROOT / "docs" / "step_functions_jsonata"
    script_dir = REPO_ROOT / "src" / "ndr" / "scripts"

    assert_contract_surface(
        matrix=matrix,
        edge_id="monthly_machine_inventory",
        layer="pipeline_params",
        observed_fields=_extract_pipeline_params(sfn_dir / "sfn_ndr_monthly_fg_b_baselines.json", "StartMachineInventoryRefresh"),
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="monthly_machine_inventory",
        layer="cli_args",
        observed_fields=_extract_cli_args(script_dir / "run_machine_inventory_unload.py"),
    )

    assert_contract_surface(
        matrix=matrix,
        edge_id="rt_15m_core",
        layer="pipeline_params",
        observed_fields=_extract_pipeline_params(sfn_dir / "sfn_ndr_15m_features_inference.json", "Start15mFeaturesPipeline"),
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="rt_15m_core",
        layer="cli_args",
        observed_fields=_extract_cli_args(script_dir / "run_delta_builder.py"),
    )

    assert_contract_surface(
        matrix=matrix,
        edge_id="backfill_reprocessing",
        layer="pipeline_params",
        observed_fields=_extract_pipeline_params(sfn_dir / "sfn_ndr_backfill_reprocessing.json", "StartBackfillPipeline"),
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="backfill_reprocessing",
        layer="cli_args",
        observed_fields=_extract_cli_args(script_dir / "run_backfill_reprocessing_executor.py"),
    )

    assert_contract_surface(
        matrix=matrix,
        edge_id="training_if_pipeline",
        layer="pipeline_params",
        observed_fields=_extract_pipeline_params(sfn_dir / "sfn_ndr_training_orchestrator.json", "StartTrainingPipeline"),
    )
    assert_contract_surface(
        matrix=matrix,
        edge_id="training_if_pipeline",
        layer="cli_args",
        observed_fields=_extract_cli_args(script_dir / "run_if_training.py"),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
