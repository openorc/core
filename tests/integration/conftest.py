"""Shared fixtures for the integration-marked persistence suites.

Every integration module in this directory consumes the explicitly supplied
non-production Supabase branch database and proves durable invariants against
the real Phase 1 schema. They are excluded from the ordinary deterministic
baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \
      .venv/bin/python -m pytest -m integration tests/integration

The suite consumes the database it is given and never provisions one.
Provisioning and teardown of the target sit outside the test suite and outside
agent responsibility: in the normal Owner local-development flow the target is
the ephemeral non-production Supabase branch that the Owner-only
``devserver.sh`` command creates and later deletes (agents never invoke it).

The session-scoped ``migrated_database`` fixture resets the ``openorc`` schema
and applies the committed migrations from scratch within the supplied database
ONCE per pytest session, so a normal full ``tests/integration`` run does not
replay the whole migration set per module. Running any individual integration
module directly still works: pytest applies this ``conftest.py`` to module-rooted
runs the same way. The function-scoped ``conn`` fixture preserves per-test
transaction rollback/isolation, so no committed test state leaks between
tests regardless of which module runs.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection, InterfaceError, OperationalError, connect

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("OPENORC_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("OPENORC_TEST_DATABASE_URL is not configured")
    return url


# Hosted Supabase infrastructure occasionally terminates the bootstrap session
# server-side during the from-scratch schema reset (psycopg.errors.AdminShutdown,
# SQLSTATE 57P01; observed on both pooler ports during PR #277 validation). Each
# attempt opens a fresh connection and performs the complete reset, which is
# idempotent by construction, so transient connection terminations are retried
# a bounded number of times with short backoff. Migration-file SQL errors are
# never retried.
_BOOTSTRAP_ATTEMPTS = 3
_BOOTSTRAP_BACKOFF_SECONDS = (1.0, 2.0)


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Reset the openorc schema and apply all committed migrations, once per session."""
    last_error: Exception | None = None
    for attempt in range(1, _BOOTSTRAP_ATTEMPTS + 1):
        try:
            with connect(database_url) as conn:
                conn.execute("drop schema if exists openorc cascade")
                for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                    # Committed migration files are trusted repository content applied
                    # wholesale; LiteralString is the driver's injection-safe query
                    # contract, satisfied here by repository-controlled file text.
                    migration_sql = cast(LiteralString, migration_path.read_text(encoding="utf-8"))
                    conn.execute(migration_sql)
                conn.commit()
            return database_url
        except (OperationalError, InterfaceError) as error:
            # The psycopg connection-termination family, which includes
            # AdminShutdown (57P01); migration-file SQL errors are not in this
            # family and therefore fail loudly without retry.
            last_error = error
            if attempt < _BOOTSTRAP_ATTEMPTS:
                delay = _BOOTSTRAP_BACKOFF_SECONDS[attempt - 1]
                print(
                    f"[integration-bootstrap] transient connection termination during "
                    f"the schema reset (attempt {attempt}/{_BOOTSTRAP_ATTEMPTS}): "
                    f"{type(error).__name__}; retrying from scratch in {delay:g}s",
                    flush=True,
                )
                time.sleep(delay)
    assert last_error is not None
    raise last_error


@pytest.fixture
def conn(migrated_database: str) -> Iterator[Connection[Any]]:
    """One connection per test; each test runs inside one rolled-back transaction."""
    with connect(migrated_database) as connection:
        yield connection
        connection.rollback()
