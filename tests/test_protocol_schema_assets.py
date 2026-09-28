"""Canonical schema asset loading/rendering tests (issue #65).

Ordinary deterministic tests: no database, no network. The five shipped JSON
Schema files are the sole machine-contract source of truth; these tests prove
the schema-asset helpers load and render exactly those assets (light shape
assertions only — the schema definitions themselves are never duplicated in
test code).
"""

from __future__ import annotations

import json

import pytest

from openorc.protocol import schema_assets
from openorc.protocol.models import FORMAL_RESPONSE_FAMILIES


def test_every_family_loads_its_canonical_asset():
    for family in sorted(FORMAL_RESPONSE_FAMILIES):
        schema = schema_assets.load_schema(family)
        assert schema["properties"]["type"]["const"] == family
        assert schema["properties"]["schema_version"]["const"] == 1


def test_render_returns_canonical_asset_text_that_round_trips():
    for family in sorted(FORMAL_RESPONSE_FAMILIES):
        rendered = schema_assets.render_schema(family)
        assert json.loads(rendered) == schema_assets.load_schema(family)


def test_load_returns_a_fresh_dictionary_on_every_call():
    first = schema_assets.load_schema("plan_result")
    second = schema_assets.load_schema("plan_result")
    assert first == second
    assert first is not second


def test_unknown_family_is_rejected():
    with pytest.raises(ValueError):
        schema_assets.load_schema("not_a_family")
    with pytest.raises(ValueError):
        schema_assets.render_schema("not_a_family")
