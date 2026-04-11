from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate code artifact manifest/hash contracts")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--feature-spec-version", required=True)
    parser.add_argument("--artifact-build-id", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(
        f"validation_started project={args.project_name} version={args.feature_spec_version} build={args.artifact_build_id}"
    )


if __name__ == "__main__":
    main()
