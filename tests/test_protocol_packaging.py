"""Installed-artifact packaging test for the canonical schema assets (issue #65).

The acceptance criterion is that the five canonical v1 JSON Schema assets are
loadable from an installed-package path through the schema-asset helpers — not
merely present in the repository or a ZIP listing. This test builds the
project wheel, installs it with ``--no-deps`` into an isolated throwaway venv
(offline: the wheel is built locally), and runs a subprocess from a neutral
working directory against the installed artifact that loads and renders every
canonical family and asserts the resources resolve inside site-packages.

The exercised import chain is dependency-free by design:
``openorc/__init__.py`` carries only version metadata,
``openorc/protocol/__init__.py`` is documentation only, and
``openorc/protocol/schema_assets.py`` imports only the standard library. If
that changes, this test fails and the artifact check must install the wheel's
dependencies explicitly.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from openorc.protocol.models import FORMAL_RESPONSE_FAMILIES

_REPO_ROOT = Path(__file__).resolve().parents[1]

_ARTIFACT_CHECK_SCRIPT = """
import json
from pathlib import Path

from openorc import protocol
from openorc.protocol import schema_assets

package_dir = Path(protocol.__file__).resolve().parent
assert "site-packages" in str(package_dir), "not the installed artifact: " + str(package_dir)

families = {families!r}
for family in families:
    schema = schema_assets.load_schema(family)
    assert schema["properties"]["type"]["const"] == family, family
    assert json.loads(schema_assets.render_schema(family)) == schema, family

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
    script = _ARTIFACT_CHECK_SCRIPT.format(families=tuple(sorted(FORMAL_RESPONSE_FAMILIES)))
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
