"""
conftest.py — Shared pytest fixtures for all test files.

PURPOSE:
    This file is automatically detected by pytest. Any fixture defined here
    is available to ALL test files in the tests/ directory without importing it.

    The key fixture is `spark` — a local SparkSession used by every test.
    It is created once per test session (not per test) to save startup time.

HOW IT WORKS:
    When you write `def test_something(spark):` in any test file, pytest
    automatically injects the SparkSession from this fixture.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a single local SparkSession shared across ALL tests.

    scope="session" means:
      - Spark starts ONCE when the first test runs
      - ALL tests reuse the same SparkSession
      - Spark shuts down AFTER the last test finishes
      - This saves ~10-15 seconds per test run vs. creating one per test
    """
    return (
        SparkSession.builder
        .appName("maven-market-tests")
        .getOrCreate()
    )
