"""Run Task-15 startup observability and rollback gate from JSON evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ndr.orchestration.contract_validators import evaluate_task15_initial_deployment_observability


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate startup observability/rollback evidence.")
    parser.add_argument(
        "--evidence-path",
        default="docs/archive/debug_records/task15_startup_observability_bundle.json",
        help="Path to startup observability JSON evidence.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    evidence_path = Path(args.evidence_path)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    report = evaluate_task15_initial_deployment_observability(evidence=evidence)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI behavior
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
