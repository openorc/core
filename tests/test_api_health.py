"""Tests for the OpenOrc API application factory and health transport."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import FastAPI
from fastapi.testclient import TestClient

from openorc import __version__
from openorc.api.app import app as module_app
from openorc.api.app import create_app
from openorc.config import Settings


def test_module_level_app_is_compatible_with_uvicorn_import_string() -> None:
    assert isinstance(module_app, FastAPI)


def test_create_app_uses_injected_settings(settings_factory: Callable[..., Settings]) -> None:
    settings = settings_factory(api_port=3998)

    app = create_app(settings)

    assert isinstance(app, FastAPI)
    assert app.state.settings is settings


def test_create_app_instances_are_independent(settings_factory: Callable[..., Settings]) -> None:
    first = create_app(settings_factory())
    second = create_app(settings_factory())

    assert first is not second


def test_healthz_returns_deterministic_payload(settings_factory: Callable[..., Settings]) -> None:
    client = TestClient(create_app(settings_factory()))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "openorc-api",
        "version": __version__,
    }
