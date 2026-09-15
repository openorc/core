"""Time normalization for the persistence boundary.

Instants cross the persistence boundary as timezone-aware ``datetime`` values
normalized to UTC. These are low-level utilities only; workflow-clock
semantics, if any, belong to the domain, not here.
"""

from __future__ import annotations

from datetime import UTC, datetime

__all__ = ["NaiveDatetimeError", "normalize_utc", "utc_now"]


class NaiveDatetimeError(ValueError):
    """Raised when a naive datetime reaches the persistence boundary."""


def utc_now() -> datetime:
    """Return the current instant as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def normalize_utc(value: datetime) -> datetime:
    """Normalize a timezone-aware datetime to UTC, preserving the instant.

    Naive datetimes are rejected: the persistence boundary never guesses a
    timezone for a value that did not carry one.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise NaiveDatetimeError(
            "naive datetimes are rejected at the persistence boundary; "
            "values must be timezone-aware"
        )
    return value.astimezone(UTC)
