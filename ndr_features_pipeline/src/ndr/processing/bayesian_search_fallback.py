"""NDR bayesian search fallback module."""

from __future__ import annotations


from itertools import product
from math import erf, exp, pi, sqrt
from random import Random
from typing import Any, Callable, Dict, List, Tuple


def _normal_pdf(x: float) -> float:
    """Execute the normal pdf stage of the workflow."""
    return (1.0 / sqrt(2.0 * pi)) * exp(-0.5 * x * x)


def _normal_cdf(x: float) -> float:
    """Execute the normal cdf stage of the workflow."""
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _distance(a: Tuple[Any, ...], b: Tuple[Any, ...]) -> float:
    """Execute the distance stage of the workflow."""
    return float(sum(0.0 if x == y else 1.0 for x, y in zip(a, b)))


def _surrogate_stats(
    candidate_key: Tuple[Any, ...],
    observed: List[Tuple[Tuple[Any, ...], float]],
) -> Tuple[float, float]:
    # Simple kernel-weighted surrogate model over categorical coordinates.
    """Execute the surrogate stats stage of the workflow."""
    weights: List[float] = []
    ys: List[float] = []
    for key, y in observed:
        d = _distance(candidate_key, key)
        w = exp(-d)
        weights.append(w)
        ys.append(y)

    w_sum = sum(weights)
    if w_sum <= 1e-12:
        mean = sum(ys) / max(len(ys), 1)
        var = 1.0
        return mean, sqrt(var)

    mean = sum(w * y for w, y in zip(weights, ys)) / w_sum
    var = sum(w * ((y - mean) ** 2) for w, y in zip(weights, ys)) / w_sum
    std = sqrt(max(var, 1e-6))
    return mean, std


def run_bayesian_search(
    search_space: Dict[str, List[Any]],
    evaluate_fn: Callable[[Dict[str, Any]], Dict[str, float]],
    max_trials: int,
    seed: int,
    initial_params: Dict[str, Any] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Basic Bayesian-like optimizer for discrete spaces without external deps."""

    rng = Random(seed)
    param_names = sorted(search_space.keys())

    all_candidates: List[Dict[str, Any]] = []
    for values in product(*[search_space[name] for name in param_names]):
        all_candidates.append({name: value for name, value in zip(param_names, values)})

    trials: List[Dict[str, Any]] = []
    seen = set()

    def key_for(params: Dict[str, Any]) -> Tuple[Any, ...]:
        """Execute the key for stage of the workflow."""
        return tuple(params[name] for name in param_names)

    def evaluate(params: Dict[str, Any]) -> None:
        """Execute the evaluate stage of the workflow."""
        score = evaluate_fn(params)
        trials.append({"trial": len(trials), "params": params, **score})
        seen.add(key_for(params))

    if initial_params is not None:
        evaluate(initial_params)

    while len(trials) < max_trials and len(seen) < len(all_candidates):
        unseen = [c for c in all_candidates if key_for(c) not in seen]
        if not unseen:
            break

        if len(trials) < min(5, max_trials):
            evaluate(unseen[rng.randrange(len(unseen))])
            continue

        observed = [(key_for(t["params"]), float(t["objective"])) for t in trials]
        best = min(y for _, y in observed)

        best_idx = 0
        best_ei = -1.0
        for idx, cand in enumerate(unseen):
            mu, sigma = _surrogate_stats(key_for(cand), observed)
            z = (best - mu) / max(sigma, 1e-9)
            ei = (best - mu) * _normal_cdf(z) + sigma * _normal_pdf(z)
            if ei > best_ei:
                best_ei = ei
                best_idx = idx

        evaluate(unseen[best_idx])

    best_trial = min(trials, key=lambda t: t["objective"])
    return trials, best_trial
