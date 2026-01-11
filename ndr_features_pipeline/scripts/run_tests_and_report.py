"""Test runner script that executes unit tests and uploads a report to S3."""

import io
import os
import sys
import unittest
from datetime import datetime

import boto3


def main() -> None:
    # Discover and run tests under ./tests
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir="tests", pattern="test_*.py")

    stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=2)
    result = runner.run(suite)

    report = stream.getvalue()
    summary_lines = [
        f"Run at: {datetime.utcnow().isoformat()}Z",
        f"Tests run: {result.testsRun}",
        f"Failures: {len(result.failures)}",
        f"Errors: {len(result.errors)}",
        "",
        "Detailed report:",
        report,
    ]
    report_text = "\n".join(summary_lines)

    # Upload report to S3
    bucket = os.environ.get("NDR_TEST_RESULTS_BUCKET", "ndr-test-results")
    key = f"test_reports/test_report_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.txt"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=report_text.encode("utf-8"))

    # Also print to stdout for CloudWatch
    print(report_text)

    # Non-zero exit code if tests failed
    if not result.wasSuccessful():
        sys.exit(1)


if __name__ == "__main__":
    main()
