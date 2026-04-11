import json
from pathlib import Path


SFN_PATH = Path("docs/step_functions_jsonata/sfn_ndr_backfill_reprocessing.json")


def _load() -> dict:
    return json.loads(SFN_PATH.read_text(encoding="utf-8"))


def test_backfill_map_reads_extractor_manifest_items_not_raw_payload_windows():
    doc = _load()
    states = doc["States"]

    succeeded = next(
        c for c in states["ExtractorPipelineStatusChoice"]["Choices"] if c["Condition"] == "{% $extractor_pipeline_status = 'Succeeded' %}"
    )
    assert succeeded["Next"] == "ReadExtractorManifest"
    assert states["ResolveBackfillWindows"]["Assign"]["windows"] == "{% $extractor_manifest.map_items %}"


def test_manifest_map_item_shape_is_deterministic_and_family_range_driven():
    selector = _load()["States"]["RunBackfillWindows"]["ItemSelector"]
    assert selector == {
        "project_name": "{% $project_name %}",
        "feature_spec_version": "{% $feature_spec_version %}",
        "family": "{% $states.context.Map.Item.Value.family %}",
        "range_start_ts": "{% $states.context.Map.Item.Value.range_start_ts %}",
        "range_end_ts": "{% $states.context.Map.Item.Value.range_end_ts %}",
    }

    params = _load()["States"]["RunBackfillWindows"]["ItemProcessor"]["States"]["StartBackfillPipeline"]["Arguments"]["PipelineParameters"]
    names = {item["Name"] for item in params}
    assert names == {"ProjectName", "FeatureSpecVersion", "ArtifactFamily", "RangeStartTsIso", "RangeEndTsIso"}


def test_manifest_read_and_map_failure_paths_are_deterministic():
    states = _load()["States"]

    read_manifest = states["ReadExtractorManifest"]
    assert read_manifest["Resource"] == "arn:aws:states:::aws-sdk:s3:getObject"
    assert read_manifest["Retry"][0]["MaxAttempts"] == 4

    validate_manifest = states["ValidateExtractorManifest"]
    assert validate_manifest["Default"] == "ResolveBackfillWindows"
    assert all(choice["Next"] == "FailInvalidExtractorManifest" for choice in validate_manifest["Choices"])
    assert any("requested_families" in choice["Condition"] for choice in validate_manifest["Choices"])

    assert states["FailInvalidExtractorManifest"]["Type"] == "Fail"
    assert states["RunBackfillWindows"]["ItemProcessor"]["States"]["BackfillPipelineStatusChoice"]["Default"] == "BackfillWindowFailed"
    assert states["RunBackfillWindows"]["Next"] == "BackfillCompletionVerifier"
    assert states["BackfillCompletionVerifier"]["Next"] == "BackfillCompletionChoice"
    assert states["BackfillCompletionChoice"]["Default"] == "FailBackfillCompletionContract"
    assert states["FailBackfillCompletionContract"]["Error"] == "BackfillCompletionContractError"


def test_sfn_passes_requested_families_into_extractor_and_verifier():
    states = _load()["States"]

    assert states["ResolvePipelineRuntimeParams"]["Assign"]["requested_families_csv"] == "{% $exists($states.input.requested_families[0]) ? $join($states.input.requested_families, ',') : '' %}"
    params = states["StartHistoricalWindowsExtractorPipeline"]["Arguments"]["PipelineParameters"]
    requested = next(item for item in params if item["Name"] == "RequestedFamilies")
    assert requested["Value"] == "{% $requested_families_csv %}"

    assert states["BackfillCompletionVerifier"]["Assign"]["requested_families"] == "{% $extractor_manifest.requested_families %}"
