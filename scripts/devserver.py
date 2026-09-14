#!/usr/bin/env python3
"""Thin executable entrypoint for the OpenOrc manual E2E devserver.

All orchestration lives in openorc.devtools.devserver so it stays importable
and deterministically testable; this file only locates the repository root and
delegates. The stable human-facing command remains scripts/devserver.sh.
"""

from __future__ import annotations

import sys
from pathlib import Path

from openorc.devtools.devserver import main


def entrypoint() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    return main(root=repo_root)


if __name__ == "__main__":
    sys.exit(entrypoint())
