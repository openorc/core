"""Deterministic mapping tests for PromptTemplateOverride repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows
and a fake pool/connection seam (mirroring the pull-request mapping
fakes) to prove row-to-domain-object mapping, UTC normalization at the
persistence boundary, parameterization, the slot upsert (identity and
``created_at`` preserved, ``updated_at`` advanced), and the reset
semantics: reset is ``DELETE ... RETURNING`` — actual row deletion, no
tombstone, no built-in default materialization. Database constraint
behavior is proven against a real database by the integration-marked
suite in ``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.prompts import PromptTemplateOverrideDomainError
from openorc.persistence.pool import DatabasePool
from openorc.persistence.prompts import (
    get_prompt_template_override,
    list_prompt_template_overrides,
    reset_prompt_template_override,
    set_prompt_template_override,
)


class FakeCursor:
    """Returns one canned row (and optional canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    """Records executed SQL and returns canned rows, optionally per statement."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        responses: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.responses = list(responses) if responses is not None else None
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if self.responses is not None:
            response = self.responses.pop(0)
            return FakeCursor(response, None if response is None else [response])
        return FakeCursor(self.row, self.rows)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[FakeConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("mapping tests never close pools")


def _authored_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 18, 12, 0, 0, tzinfo=timezone(timedelta(hours=-5)))


def _utc_authored_at() -> datetime:
    return _authored_at().astimezone(UTC)


def _override_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "template_key": "producer.plan_instructions",
        "base_template_version": "builtin-1.0.0",
        "instruction_text": "Plan the task step by step.",
        "created_at": _authored_at(),
        "updated_at": _authored_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["template_key"],
        values["base_template_version"],
        values["instruction_text"],
        values["created_at"],
        values["updated_at"],
    )


def test_set_upserts_the_slot_preserving_identity_and_created_at() -> None:
    row = _override_row(
        base_template_version="builtin-1.1.0", instruction_text="Updated instructions."
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    record = set_prompt_template_override(
        pool,
        workspace_id=row[1],
        template_key=row[2],
        base_template_version="builtin-1.1.0",
        instruction_text="Updated instructions.",
    )

    assert record.id == row[0]
    assert record.base_template_version == "builtin-1.1.0"
    assert record.created_at == _utc_authored_at()
    assert record.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.prompt_template_overrides" in sql
    # The slot upsert updates content and updated_at only: row identity
    # and created_at are never rewritten.
    assert "on conflict (workspace_id, template_key) do update" in sql
    assert "base_template_version = excluded.base_template_version" in sql
    assert "instruction_text = excluded.instruction_text" in sql
    assert "updated_at = now()" in sql
    assert "set id" not in sql
    assert "created_at =" not in sql
    assert params == (row[1], row[2], "builtin-1.1.0", "Updated instructions.")
    assert len(fake_conn.executed) == 1


def test_set_validates_payload_before_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    base: dict[str, Any] = {
        "workspace_id": uuid.uuid4(),
        "template_key": "producer.plan_instructions",
        "base_template_version": "builtin-1.0.0",
        "instruction_text": "Plan the task step by step.",
    }
    for overrides in (
        {"template_key": "   "},
        {"base_template_version": ""},
        {"instruction_text": None},
        {"workspace_id": "not-a-uuid"},
    ):
        with pytest.raises(PromptTemplateOverrideDomainError):
            set_prompt_template_override(pool, **{**base, **overrides})
    assert fake_conn.executed == []


def test_get_maps_a_row_and_normalizes_to_utc() -> None:
    row = _override_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    record = get_prompt_template_override(pool, workspace_id=row[1], template_key=row[2])

    assert record is not None
    assert record.instruction_text == "Plan the task step by step."
    assert record.created_at == _utc_authored_at()
    assert record.updated_at == _utc_authored_at()
    sql, params = fake_conn.executed[0]
    assert "select" in sql and "openorc.prompt_template_overrides" in sql
    assert params == (row[1], row[2])


def test_get_returns_none_for_an_absent_override() -> None:
    # Row absence is the representation of "use the shipped built-in
    # default" — there is no default row to return.
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))
    assert (
        get_prompt_template_override(
            pool, workspace_id=uuid.uuid4(), template_key="producer.plan_instructions"
        )
        is None
    )


def test_list_orders_by_template_key_and_is_workspace_scoped() -> None:
    rows = [
        _override_row(template_key="reviewer.plan_review_instructions"),
        _override_row(template_key="producer.plan_instructions"),
    ]
    fake_conn = FakeConnection(None, rows=rows)
    pool = cast(DatabasePool, FakePool(fake_conn))
    workspace_id = rows[0][1]

    records = list_prompt_template_overrides(pool, workspace_id=workspace_id)

    assert [r.template_key for r in records] == [
        "reviewer.plan_review_instructions",
        "producer.plan_instructions",
    ]
    sql, params = fake_conn.executed[0]
    assert "where workspace_id = %s order by template_key" in sql
    assert params == (workspace_id,)


def test_reset_deletes_the_row_and_returns_the_deleted_record() -> None:
    row = _override_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    deleted = reset_prompt_template_override(pool, workspace_id=row[1], template_key=row[2])

    assert deleted is not None
    assert deleted.id == row[0]
    assert deleted.template_key == row[2]
    assert deleted.instruction_text == row[4]
    sql, params = fake_conn.executed[0]
    # Reset is actual row deletion returning the removed record — never a
    # tombstone, never a stored built-in default.
    assert "delete from openorc.prompt_template_overrides" in sql
    assert "returning" in sql
    assert "insert" not in sql
    assert params == (row[1], row[2])


def test_reset_returns_none_when_the_slot_is_not_overridden() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))
    workspace_id = uuid.uuid4()
    assert (
        reset_prompt_template_override(
            pool, workspace_id=workspace_id, template_key="producer.plan_instructions"
        )
        is None
    )
    sql, params = fake_conn.executed[0]
    assert "delete from openorc.prompt_template_overrides" in sql
    assert params == (workspace_id, "producer.plan_instructions")
