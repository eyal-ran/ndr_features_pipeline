from ndr.processing.bayesian_search_fallback import run_bayesian_search


def test_run_bayesian_search_returns_trials_and_best():
    space = {
        "n_estimators": [100, 200],
        "max_samples": [0.5, 1.0],
        "bootstrap": [False, True],
    }

    def evaluate(params):
        objective = abs(params["n_estimators"] - 200) / 100.0
        objective += abs(params["max_samples"] - 1.0)
        objective += 0.1 if params["bootstrap"] else 0.0
        return {"objective": float(objective), "drift": 0.0, "alert_delta": 0.0}

    initial = {"n_estimators": 100, "max_samples": 0.5, "bootstrap": False}
    trials, best = run_bayesian_search(
        search_space=space,
        evaluate_fn=evaluate,
        max_trials=5,
        seed=7,
        initial_params=initial,
    )

    assert len(trials) <= 5
    assert trials[0]["params"] == initial
    assert set(best["params"].keys()) == {"n_estimators", "max_samples", "bootstrap"}
