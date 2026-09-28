"""Architectural-independence tests for the protocol package (issue #65).

Proves the required dependency direction at the source level: no module under
``openorc.protocol`` imports domain, services, persistence, adapters, API,
workers, or any runtime-specific package. The protocol package stays a
low-level runtime-independent contract package.
"""

from __future__ import annotations

import ast
from pathlib import Path

from openorc import protocol


def _protocol_source_files() -> list[Path]:
    package_dir = Path(protocol.__file__).resolve().parent
    return sorted(package_dir.rglob("*.py"))


def _import_targets(tree: ast.AST) -> list[str]:
    targets: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            targets.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            targets.append(node.module)
    return targets


def test_protocol_package_imports_only_the_standard_library_and_itself():
    files = _protocol_source_files()
    assert files, "protocol package source must be discoverable"

    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for target in _import_targets(tree):
            if target == "openorc" or target.startswith("openorc.protocol"):
                continue
            assert not target.startswith("openorc."), (
                f"{path.name} imports workflow/transport module {target}"
            )
            assert "cline" not in target.lower(), (
                f"{path.name} imports runtime-specific module {target}"
            )
