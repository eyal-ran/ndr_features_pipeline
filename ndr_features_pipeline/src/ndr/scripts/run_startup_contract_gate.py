"""Run Task-14 startup contract conformance gate from JSON evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ndr.orchestration.contract_validators import evaluate_task14_startup_contract_conformance


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate startup contract conformance evidence.")
    parser.add_argument(
        "--evidence-path",
        default="docs/archive/debug_records/task14_startup_contract_matrix.json",
        help="Path to startup contract matrix JSON evidence.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    evidence_path = Path(args.evidence_path)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    report = evaluate_task14_startup_contract_conformance(evidence=evidence)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI behavior
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
