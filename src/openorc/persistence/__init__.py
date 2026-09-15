"""Durable representation and repository/data-access mechanics for OpenOrc.

Postgres is the durable source for OpenOrc workflow/control state. The
``AGENTS.md`` in this package carries the durable driver, pooling, transaction,
and SQL conventions every persistence module inherits.
"""
