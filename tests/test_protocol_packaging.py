"""Installed-artifact packaging test for the canonical schema assets (issue #65)
and the canonical role-initialization assets (issue #66).

The acceptance criterion is that the six canonical v1 JSON Schema assets are
loadable from an installed-package path through the schema-asset helpers — not
merely present in the repository or a ZIP listing — and that the two canonical
role-initialization assets load, render their controlled schema insertion
points, and compose optional Workspace guidance from the installed artifact
too. This test builds the project wheel, installs it with ``--no-deps`` into
an isolated throwaway venv (offline: the wheel is built locally), and runs a
subprocess from a neutral working directory against the installed artifact
that loads and renders every canonical family and initialization asset and
asserts the resources resolve inside site-packages.

The exercised import chain is dependency-free by design:
``openorc/__init__.py`` carries only version metadata,
``openorc/protocol/__init__.py`` is documentation only, and the exercised
protocol modules (``models``, ``schema_assets``, ``initialization_assets``,
``controls_assets``) import only the standard library. If that changes, this
test fails and the artifact check must install the wheel's dependencies
explicitly.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from openorc import protocol
from openorc.protocol import controls_assets
from openorc.protocol.models import (
    FORMAL_RESPONSE_FAMILIES,
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    SESSION_READY_FAMILY,
    WORKSPACE_RESULT_FAMILY,
)

# Role wiring for the controlled schema insertion points (issue #66), mirrored
# as composition wiring only — never schema definitions.
_ROLE_FAMILIES = {
    "producer": (
        PLAN_RESULT_FAMILY,
        IMPLEMENTATION_RESULT_FAMILY,
        PR_RESULT_FAMILY,
        WORKSPACE_RESULT_FAMILY,
        SESSION_READY_FAMILY,
    ),
    "reviewer": (REVIEW_RESULT_FAMILY, SESSION_READY_FAMILY),
}

_REPO_ROOT = Path(__file__).resolve().parents[1]

_ARTIFACT_CHECK_SCRIPT = """
import json
import uuid
from pathlib import Path

from openorc import protocol
from openorc.protocol import controls_assets, initialization_assets, schema_assets

package_dir = Path(protocol.__file__).resolve().parent
assert "site-packages" in str(package_dir), "not the installed artifact: " + str(package_dir)

families = {families!r}
for family in families:
    schema = schema_assets.load_schema(family)
    assert schema["properties"]["type"]["const"] == family, family
    assert json.loads(schema_assets.render_schema(family)) == schema, family

expected_role_families = {expected_role_families!r}
guidance = "Owner guidance for the artifact check."
for role, role_families in expected_role_families.items():
    asset_path = package_dir / "initialization" / (role + ".md")
    asset = initialization_assets.initialization_asset_text(role)
    assert asset == asset_path.read_text(encoding="utf-8"), role
    rendered = initialization_assets.render_initialization(role)
    assert (chr(123) * 2) not in rendered, role
    for family in role_families:
        assert schema_assets.render_schema(family) in rendered, (role, family)
    assert rendered == initialization_assets.compose_initialization(role, None), role
    composed = initialization_assets.compose_initialization(role, guidance)
    delimiter = "\\n\\n---\\n\\n## Workspace guidance (Owner-authored, subordinate)\\n\\n"
    assert composed == rendered + delimiter + guidance, role

control_assets = {control_assets!r}
plain_control_assets = ("plan", "pr_compose")
workspace_control_assets = ("prepare_workspace", "cleanup_workspace")
for asset in control_assets:
    asset_path = package_dir / "controls" / (asset + ".md")
    assert controls_assets.control_asset_text(asset) == asset_path.read_text(
        encoding="utf-8"
    ), asset
for asset in plain_control_assets:
    rendered_control = controls_assets.render_control_asset(
        asset, repository_full_name="openorc/core", issue_number=129
    )
    assert (chr(123) * 2) not in rendered_control, asset
    assert "openorc/core" in rendered_control, asset
    assert "#129" in rendered_control, asset
task_uuid = uuid.UUID("00000000-0000-0000-0000-000000000181")
for asset in workspace_control_assets:
    rendered_control = controls_assets.render_control_asset(
        asset,
        repository_full_name="openorc/core",
        issue_number=181,
        task_uuid=task_uuid,
        target_directory="/tmp/openorc-task-checkout-181",
    )
    assert (chr(123) * 2) not in rendered_control, asset
    assert "openorc/core" in rendered_control, asset
    assert "#181" in rendered_control, asset
    assert str(task_uuid) in rendered_control, asset
    assert "/tmp/openorc-task-checkout-181" in rendered_control, asset

# The Cline adapter (issue #74) consumes the canonical role-prompt Markdown
# assets through a package-resource loader: prove they load from the
# installed artifact too, byte-identical to the repository content.
role_prompts = {role_prompts!r}
from importlib import resources as _resources

for role, expected in role_prompts.items():
    loaded = (
        _resources.files("openorc.protocol")
        .joinpath("role_prompts")
        .joinpath(role + ".md")
        .read_text(encoding="utf-8")
    )
    assert loaded == expected, role
    asset_path = package_dir / "role_prompts" / (role + ".md")
    assert asset_path.read_text(encoding="utf-8") == expected, role

print("PACKAGING_OK")
"""


def test_canonical_schema_assets_load_from_the_installed_wheel(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    assert uv, "uv is required to build and install the artifact-check environment"

    dist_dir = tmp_path / "dist"
    build = subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(dist_dir)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert build.returncode == 0, f"wheel build failed:\n{build.stdout}\n{build.stderr}"
    wheels = sorted(dist_dir.glob("*.whl"))
    assert len(wheels) == 1, f"expected exactly one built wheel, got {wheels}"

    venv_dir = tmp_path / "artifact-venv"
    venv_python = venv_dir / "bin" / "python"
    create = subprocess.run(
        [uv, "venv", "--python", sys.executable, str(venv_dir)],
        capture_output=True,
        text=True,
    )
    assert create.returncode == 0, f"venv creation failed:\n{create.stdout}\n{create.stderr}"
    install = subprocess.run(
        [uv, "pip", "install", "--no-deps", "--python", str(venv_python), str(wheels[0])],
        capture_output=True,
        text=True,
    )
    assert install.returncode == 0, f"wheel install failed:\n{install.stdout}\n{install.stderr}"

    neutral_cwd = tmp_path / "neutral-cwd"
    neutral_cwd.mkdir()
    protocol_package_dir = Path(protocol.__file__).resolve().parent
    role_prompts = {
        role: (protocol_package_dir / "role_prompts" / (role + ".md")).read_text(encoding="utf-8")
        for role in ("producer", "reviewer")
    }
    script = _ARTIFACT_CHECK_SCRIPT.format(
        families=tuple(sorted(FORMAL_RESPONSE_FAMILIES)),
        expected_role_families=_ROLE_FAMILIES,
        control_assets=tuple(sorted(controls_assets.CONTROL_ASSETS)),
        role_prompts=role_prompts,
    )
    artifact_check = subprocess.run(
        [str(venv_python), "-c", script],
        cwd=neutral_cwd,
        capture_output=True,
        text=True,
    )
    assert artifact_check.returncode == 0, (
        f"installed-artifact check failed:\n{artifact_check.stdout}\n{artifact_check.stderr}"
    )
    assert "PACKAGING_OK" in artifact_check.stdout
