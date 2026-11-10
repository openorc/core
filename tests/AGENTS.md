# Test Suite Agent Context

## Context traversal

Before editing tests, read the `AGENTS.md` for the subsystem under test. Cross-boundary tests must load every relevant subsystem guide.

## Test philosophy

Tests should be behavior-focused, deterministic, and independent of live infrastructure for ordinary runs. Prefer fake adapters and controlled providers over network dependence. Do not chase line coverage for its own sake.

New behavior requires appropriate tests in the same change. Bug fixes require regression coverage.


## Coverage ownership and proportionality

Before adding tests, inspect coverage owned by dependencies and adjacent layers. Follow the implementation issue's focused test requirements: prove the new capability's own behavior and material cross-boundary seams it introduces or exercises. Do not recreate established protocol, adapter, persistence, lifecycle, retry, recovery, or error-taxonomy matrices in every caller. Critical invariants must remain covered, but not independently re-proven at every layer.

Prefer representative or parameterized cases over exhaustive permutations of guards and failure modes. Add cases beyond the issue's specified coverage only for a concrete material behavior, regression, or previously unproved seam. When an established contract changes, update its owning tests and the affected seam tests rather than expanding unrelated suites. Existing tests are evidence of covered behavior, not mandatory templates for the size or structure of new tests.

## Test design and maintenance

Assert observable behavior, durable outcomes, and authority boundaries rather than incidental helper calls or query shapes. Prefer typed test builders, stable fakes, and shared fixtures where they reduce coupling. Avoid duplicating positional database-row tuples, scripting whole SQL call sequences, or extensively patching internals in ordinary service tests when a narrower seam suffices. Use SQL-level doubles or exact ordering assertions when the SQL behavior or transaction/external-effect boundary is itself under test; use real-Postgres integration coverage for database-enforced guarantees. Do not rewrite existing tests solely for style.

## High-risk boundaries

Where applicable, tests must prove session isolation/continuity, exact-subject review, runtime capacity/backpressure, malformed formal-response rejection, no blind replay after uncertain sends, stale-operation rejection, GitHub webhook dedupe + reconciliation, PR-head staleness, expected-head merge protection, cancellation semantics, Workspace isolation, and that the UI/queues/transcripts never become independent workflow authority.

Normal test suites must not silently call live production GitHub, Supabase, Valkey, Cline Hub, or model inference. Real integration/E2E layers are explicit and separately configured.

Use `tests/README.md` for commands, suite layout, fixtures, markers, setup, and troubleshooting rather than bloating this standing policy file.
