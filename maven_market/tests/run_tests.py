"""
run_tests.py — Manual test runner for Databricks.

PURPOSE:
    Execute all pytest tests from within a Databricks notebook, job task,
    or terminal. This script handles the workspace filesystem limitations
    (no __pycache__) and outputs results directly.

USAGE:
    Option 1 — Run as a notebook/cell:
        %run ../tests/run_tests

    Option 2 — Run as a Databricks job task:
        Referenced in jobs.yml as a spark_python_task

    Option 3 — Run from terminal:
        cd /Workspace/Capstone-Project-Group4/maven_market
        python tests/run_tests.py
"""

import sys
import os
import shutil
import tempfile


def run_tests():
    """
    Copy tests to /tmp (avoids workspace __pycache__ errors),
    run pytest, and return the exit code.
    """
    # Paths
    workspace_test_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__))
    )
    tmp_test_dir = os.path.join(tempfile.gettempdir(), "maven_market_tests")

    # Clean previous run
    if os.path.exists(tmp_test_dir):
        shutil.rmtree(tmp_test_dir)

    # Copy tests to /tmp to avoid workspace filesystem __pycache__ errors
    shutil.copytree(workspace_test_dir, tmp_test_dir)

    # Run pytest from /tmp
    import pytest
    exit_code = pytest.main([
        tmp_test_dir,
        "-v",
        "--tb=short",
        "-p", "no:cacheprovider",
    ])

    # Clean up
    shutil.rmtree(tmp_test_dir, ignore_errors=True)

    # Summary
    if exit_code == 0:
        print("\n✅ ALL TESTS PASSED")
    else:
        print(f"\n❌ TESTS FAILED (exit code: {exit_code})")

    return exit_code


if __name__ == "__main__":
    code = run_tests()
    # Fail the Databricks job task if tests fail
    if code != 0:
        sys.exit(code)
