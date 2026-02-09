"""Tests for feature catalog helpers."""

from ndr.catalog.feature_catalog import build_fg_a_metric_names, build_fg_c_metric_names
from ndr.model.fg_c_schema import DEFAULT_TRANSFORMS, build_default_metric_list


def test_fg_a_metric_count_matches_expectations() -> None:
    metrics = build_fg_a_metric_names()
    assert len(metrics) == 480


def test_fg_c_default_metric_count_matches_expectations() -> None:
    metrics = build_default_metric_list()
    assert len(metrics) == 440
    features = build_fg_c_metric_names(metrics=metrics, transforms=DEFAULT_TRANSFORMS)
    assert len(features) == 1320

