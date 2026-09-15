"""Deterministic tests for persistence time normalization."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone, tzinfo

import pytest

from openorc.persistence.time import NaiveDatetimeError, normalize_utc, utc_now


class _ZoneWithoutOffset(tzinfo):
    """A tzinfo whose utcoffset() is None: naive by psycopg conventions."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        return None


def test_utc_now_is_timezone_aware_utc() -> None:
    value = utc_now()

    assert value.tzinfo is not None
    assert value.utcoffset() == timedelta(0)


def test_normalize_utc_converts_other_zones_to_utc() -> None:
    zone = timezone(timedelta(hours=2))
    value = datetime(2026, 9, 15, 12, 0, 0, tzinfo=zone)

    normalized = normalize_utc(value)

    assert normalized.tzinfo is UTC
    assert normalized.hour == 10
    assert normalized == value  # the same instant, expressed in UTC


def test_normalize_utc_keeps_utc_values_unchanged() -> None:
    value = datetime(2026, 9, 15, 0, 0, 0, tzinfo=UTC)

    assert normalize_utc(value) == value
    assert normalize_utc(value).tzinfo is UTC


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 9, 15, 12, 0, 0),  # no tzinfo at all
        datetime(2026, 9, 15, 12, 0, 0, tzinfo=_ZoneWithoutOffset()),
    ],
)
def test_normalize_utc_rejects_naive_datetimes(value: datetime) -> None:
    with pytest.raises(NaiveDatetimeError):
        normalize_utc(value)
