"""Shared fixtures for OpenOrc tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from openorc.config import Settings

_TEST_SETTINGS: dict[str, Any] = {
    "environment": "test",
    "api_host": "127.0.0.1",
    "api_port": 3999,
    "api_reload": False,
    "valkey_url": "redis://127.0.0.1:6379/0",
    "database_url": "postgresql://postgres:postgres@127.0.0.1:54322/postgres",
    "db_pool_min": 1,
    "db_pool_max": 5,
    "db_pool_timeout": 5.0,
}


@pytest.fixture
def settings_factory() -> Callable[..., Settings]:
    """Return a factory that builds test Settings with optional overrides."""

    def factory(**overrides: Any) -> Settings:
        kwargs: dict[str, Any] = dict(_TEST_SETTINGS)
        kwargs.update(overrides)
        return Settings(**kwargs)

    return factory
