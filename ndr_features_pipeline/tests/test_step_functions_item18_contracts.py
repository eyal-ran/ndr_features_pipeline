import json
from pathlib import Path


def _load(path: str):
    return json.loads(Path(path).read_text())


def test_inference_step_function_has_path_derived_batch_and_lock_key_contract():
    doc = _load("docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json")
    parse_assign = doc["States"]["ParseIncomingProjectContext"]["Assign"]
    resolve_assign = doc["States"]["ResolvePipelineRuntimeParams"]["Assign"]

    assert "parsed_project_name_from_path" in parse_assign
    assert "parsed_batch_folder" in parse_assign
    assert "parsed_source_ts_iso" in parse_assign
    assert "source_timestamp_iso" in resolve_assign
    assert "$fromMillis" in resolve_assign["batch_start_ts_iso"]

    lock_state = doc["States"]["AcquireMiniBatchLock"]
    pk_expr = lock_state["Arguments"]["Item"]["pk"]["S"]
    sk_expr = lock_state["Arguments"]["Item"]["sk"]["S"]
    assert "$project_name" in pk_expr and "$feature_spec_version" in pk_expr
    assert "$batch_start_ts_iso" in sk_expr and "$batch_end_ts_iso" in sk_expr


def test_backfill_step_function_wires_preliminary_extractor_before_map():
    doc = _load("docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json")
    states = doc["States"]
    assert "StartHistoricalWindowsExtractorPipeline" in states
    assert "WaitForHistoricalWindowsExtractor" in states

    start = states["StartHistoricalWindowsExtractorPipeline"]
    assert start["Arguments"]["PipelineName"] == "${PipelineNameBackfillHistoricalExtractor}"
    assert states["ResolvePipelineRuntimeParams"]["Next"] == "StartHistoricalWindowsExtractorPipeline"
    assert states["WaitForHistoricalWindowsExtractor"]["Next"] == "RunBackfillWindows"
    assert states["RunBackfillWindows"]["Items"] == "{% $windows %}"
