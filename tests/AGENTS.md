# Test Suite Agent Context

## Context traversal

Before editing tests, read the `AGENTS.md` for the subsystem under test. Cross-boundary tests must load every relevant subsystem guide.

## Test philosophy

Tests should be behavior-focused, deterministic, and independent of live infrastructure for ordinary runs. Prefer fake adapters and controlled providers over network dependence. Do not chase line coverage for its own sake.

New behavior requires appropriate tests in the same change. Bug fixes require regression coverage.

## High-risk boundaries

Where applicable, tests must prove session isolation/continuity, exact-subject review, runtime capacity/backpressure, malformed formal-response rejection, no blind replay after uncertain sends, stale-operation rejection, GitHub webhook dedupe + reconciliation, PR-head staleness, expected-head merge protection, cancellation semantics, Workspace isolation, and that the UI/queues/transcripts never become independent workflow authority.

Normal test suites must not silently call live production GitHub, Supabase, Valkey, Cline Hub, or model inference. Real integration/E2E layers are explicit and separately configured.

Use `tests/README.md` for commands, suite layout, fixtures, markers, setup, and troubleshooting rather than bloating this standing policy file.
