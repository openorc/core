"""Tests for the OpenOrc RQ queue naming conventions."""

from __future__ import annotations

from openorc.workers.queues import DEFAULT_QUEUE_NAMES, QUEUE_PREFIX, queue_name


def test_queue_prefix_is_the_stable_openorc_prefix() -> None:
    assert QUEUE_PREFIX == "openorc"


def test_queue_names_derive_from_the_prefix_convention() -> None:
    assert queue_name("default") == "openorc:default"
    assert queue_name("dispatch") == "openorc:dispatch"


def test_default_queue_names_are_the_canonical_default_queue() -> None:
    assert DEFAULT_QUEUE_NAMES == ("openorc:default",)
