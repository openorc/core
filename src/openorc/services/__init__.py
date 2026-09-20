"""Application services: deterministic use-case orchestration for OpenOrc.

Services own applying domain rules, loading and updating durable state,
validating current subject/authority context, calling persistence and
adapters, deciding workflow consequences, and scheduling follow-up work.
API routers and RQ jobs remain thin transports over these capabilities.

The ``AGENTS.md`` in this package carries the service organization,
exact-subject/replay-safety, and workflow ownership conventions every
service module inherits.
"""
