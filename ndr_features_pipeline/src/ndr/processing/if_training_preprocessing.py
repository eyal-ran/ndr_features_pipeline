from __future__ import annotations

from typing import Dict, Iterable, List, Tuple


def split_metadata_and_feature_columns(columns: Iterable[str], join_keys: List[str]) -> Tuple[List[str], List[str]]:
    metadata = set(join_keys + ["feature_spec_version", "dt", "mini_batch_id"])
    feature_columns = [col for col in columns if col not in metadata]
    return sorted(metadata.intersection(set(columns))), sorted(feature_columns)


def _quantile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    frac = pos - low
    return ordered[low] * (1 - frac) + ordered[high] * frac


def robust_statistics_by_feature(records: Iterable[Dict[str, float]], feature_columns: List[str]) -> Dict[str, Dict[str, float]]:
    values_by_feature: Dict[str, List[float]] = {f: [] for f in feature_columns}
    for row in records:
        for feature in feature_columns:
            value = row.get(feature)
            if value is None:
                continue
            values_by_feature[feature].append(float(value))

    stats: Dict[str, Dict[str, float]] = {}
    for feature in feature_columns:
        values = values_by_feature[feature]
        median = _quantile(values, 0.5)
        p25 = _quantile(values, 0.25)
        p75 = _quantile(values, 0.75)
        mad_values = [abs(v - median) for v in values]
        mad = _quantile(mad_values, 0.5)
        stats[feature] = {
            "median": median,
            "iqr": p75 - p25,
            "mad": mad,
        }
    return stats
