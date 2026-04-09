import json
from pathlib import Path


RT_STEP_FUNCTION_PATH = Path("docs/step_functions_jsonata/sfn_ndr_15m_features_inference.json")


def _load_states() -> dict:
    doc = json.loads(RT_STEP_FUNCTION_PATH.read_text(encoding="utf-8"))
    return doc["States"]["RunPerMlProjectBranch"]["ItemProcessor"]["States"]


def test_task13_cold_start_path_runs_core_then_readiness_then_dependent_phase():
    states = _load_states()

    assert states["FeaturesPipelineStatusChoice"]["Choices"][0]["Next"] == "BuildRtArtifactReadinessManifest"
    assert states["CheckRtArtifactReadiness"]["Choices"][1]["Next"] == "BuildRtBackfillRemediationRequest"
    assert states["UpdateBatchIndexRemediationSucceeded"]["Next"] == "RecheckRtArtifactReadiness"
    assert states["RecheckRtArtifactReadiness"]["Next"] == "CheckRtArtifactReadiness"
    assert states["CheckRtArtifactReadiness"]["Choices"][0]["Next"] == "Start15mDependentFeaturesPipeline"


def test_task13_steady_state_bypasses_remediation_and_starts_dependent_phase_directly():
    states = _load_states()

    no_missing = next(
        choice
        for choice in states["CheckRtArtifactReadiness"]["Choices"]
        if choice["Condition"] == "{% $count($rt_artifact_readiness_manifest.missing_ranges) = 0 %}"
    )
    assert no_missing["Next"] == "Start15mDependentFeaturesPipeline"
    assert states["Start15mDependentFeaturesPipeline"]["Next"] == "Describe15mDependentFeaturesPipeline"


def test_task13_failure_paths_fail_fast_for_remediation_or_dependent_phase_errors():
    states = _load_states()

    assert states["CheckRtArtifactReadiness"]["Default"] == "FailRtArtifactsUnresolvedAfterRemediation"
    assert states["UpdateBatchIndexRemediationFailed"]["Next"] == "BranchFailed"
    assert states["DependentFeaturesPipelineStatusChoice"]["Default"] == "BranchFailed"
