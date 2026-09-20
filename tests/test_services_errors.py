"""Deterministic tests for the typed application error vocabulary (issue #51).

The vocabulary is transport-neutral by construction: expected application
failures surface as these typed errors and the transport that catches them
owns the translation. These tests prove the category hierarchy (stale
operation as a distinct conflict subtype; known external failure and
uncertain external outcome as distinct siblings), that categories do not
shadow each other, that instances carry no secret-bearing state, and that
the whole service foundation imports without FastAPI, RQ, Supabase, or
GitHub client objects.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest

from openorc.services.errors import (
    ApplicationError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)

_CATEGORY_ERRORS: tuple[type[ApplicationError], ...] = (
    AuthenticationError,
    AuthorizationError,
    NotFoundError,
    InvalidCommandError,
    ConflictError,
    StaleOperationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
)


@pytest.mark.parametrize("error_type", _CATEGORY_ERRORS)
def test_every_category_is_a_transport_neutral_application_error(
    error_type: type[ApplicationError],
) -> None:
    error = error_type("boom")
    assert isinstance(error, ApplicationError)
    assert str(error) == "boom"


def test_stale_operation_is_a_distinct_conflict_subtype() -> None:
    stale: ApplicationError = StaleOperationError("state token no longer matches")
    assert isinstance(stale, ConflictError)
    with pytest.raises(ConflictError):
        raise stale


def test_external_known_failure_and_uncertainty_are_distinct_siblings() -> None:
    failed: ApplicationError = ExternalOperationFailedError("merge rejected")
    uncertain: ApplicationError = ExternalOperationUncertainError("dispatch timed out")
    assert not isinstance(failed, ExternalOperationUncertainError)
    assert not isinstance(uncertain, ExternalOperationFailedError)


def test_categories_do_not_shadow_each_other() -> None:
    instances = {error_type: error_type("x") for error_type in _CATEGORY_ERRORS}
    for error_type, instance in instances.items():
        for other_type in _CATEGORY_ERRORS:
            if other_type is error_type:
                continue
            if error_type is StaleOperationError and other_type is ConflictError:
                continue  # the one deliberate subtype relationship
            assert not isinstance(instance, other_type), (
                f"{error_type.__name__} unexpectedly matches {other_type.__name__}"
            )


def test_error_instances_carry_no_secret_bearing_state() -> None:
    for error_type in _CATEGORY_ERRORS:
        instance = error_type("boom")
        # The vocabulary is message-only today. Later capabilities add
        # explicit safe keyword fields — never credentials or tokens, and
        # never a second copy of canonical domain state.
        assert instance.__dict__ == {}


def test_service_foundation_imports_without_transport_or_runtime_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Import the service foundation fresh with transport/runtime names blocked.

    ``None`` in ``sys.modules`` makes any import of the blocked name raise
    ImportError, so this proves the foundation carries no FastAPI, RQ,
    Supabase, or GitHub client dependency.
    """
    for name in ("fastapi", "rq", "supabase", "github"):
        monkeypatch.setitem(sys.modules, name, None)

    service_modules = (
        "openorc.services",
        "openorc.services.errors",
        "openorc.services.transaction_composition",
    )
    saved: dict[str, ModuleType] = {}
    for name in service_modules:
        module = sys.modules.pop(name, None)
        if module is not None:
            saved[name] = module
    try:
        errors_module = importlib.import_module("openorc.services.errors")
        composition_module = importlib.import_module("openorc.services.transaction_composition")
        assert errors_module.ApplicationError is not None
        assert composition_module.composed_transaction is not None
    finally:
        for name, module in saved.items():
            sys.modules[name] = module
