"""OpenOrc test suite package marker.

Marks the ``tests`` tree as importable packages (``tests.fakes``,
``tests.integration``) so pytest collection resolves the dotted test-support
imports under every invocation style, not only ``python -m pytest`` run from
the repository root.
"""
