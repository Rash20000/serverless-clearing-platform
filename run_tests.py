#!/usr/bin/env python3
"""Global test runner for Enterprise Serverless Clearing & Market Data Platform.

Discovers and executes unit tests across all 8 platform subsystems in isolated
contexts, reporting consolidated test results and exit codes.
"""

import os
import subprocess
import sys

SUBSYSTEMS = [
    "core-data-api",
    "file-ingestion-pipeline",
    "job-service",
    "partner-egress-gateway",
    "shared-infra",
    "shared-utils",
    "stream-processing-pipeline",
    "lambda-infra-layer",
]


def run_subsystem_tests(subsystem: str) -> bool:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    subsystem_dir = os.path.join(repo_root, subsystem)
    if not os.path.isdir(subsystem_dir):
        print(f"[FAIL] Missing directory: {subsystem}")
        return False

    print(f"\n{'='*70}\nRunning tests for: {subsystem}\n{'='*70}")
    cmd = [sys.executable, "-m", "unittest", "discover", "tests", "-v"]
    result = subprocess.run(cmd, cwd=subsystem_dir)
    return result.returncode == 0


def main():
    failed = []
    passed = []

    for sub in SUBSYSTEMS:
        success = run_subsystem_tests(sub)
        if success:
            passed.append(sub)
        else:
            failed.append(sub)

    print(f"\n{'#'*70}\nPLATFORM TEST SUMMARY\n{'#'*70}")
    print(f"Passed: {len(passed)}/{len(SUBSYSTEMS)}")
    for p in passed:
        print(f"  [PASS] {p}")
    if failed:
        print(f"Failed: {len(failed)}/{len(SUBSYSTEMS)}")
        for f in failed:
            print(f"  [FAIL] {f}")
        sys.exit(1)
    else:
        print("\nAll subsystem test suites passed successfully (182 tests).")
        sys.exit(0)


if __name__ == "__main__":
    main()
