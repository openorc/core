# Test Suite Agent Context

## Context traversal

Before editing tests, read the `AGENTS.md` for the subsystem under test. Cross-boundary tests must load every relevant subsystem guide.

## Test philosophy and coverage ownership

Tests should be behavior-focused, deterministic, and independent of live infrastructure for ordinary runs. Prefer fake adapters and controlled providers over network dependence. Do not chase line coverage for its own sake.

New behavior requires appropriate tests in the same change. Bug fixes require regression coverage. Before adding tests, inspect what dependencies and adjacent layers already prove. Follow the implementation leaf's focused test requirements: test its own behavior and critical new or exercised cross-boundary seams, not established protocol, adapter, persistence, lifecycle, retry, recovery, or error-taxonomy matrices again.

Prefer representative or parameterized cases over exhaustive permutations. Add coverage beyond the leaf's stated tests only for a concrete material risk or uncovered contract. When a contract changes, update its owning tests and affected seams rather than expanding unrelated suites. Existing tests demonstrate established behavior; they are not mandatory templates for test count or fixture structure.

## High-risk boundaries

The following are **suite-wide guarantees**, not a checklist to re-prove in every implementation leaf. Tests in the appropriate owning layers must prove session isolation/continuity, exact-subject review, runtime capacity/backpressure, malformed formal-response rejection, no blind replay after uncertain sends, stale-operation rejection, GitHub webhook dedupe + reconciliation, PR-head staleness, expected-head merge protection, cancellation semantics, Workspace isolation, and that the UI/queues/transcripts never become independent workflow authority. New or changed seams that could violate these guarantees need focused coverage.

## Test design and execution

Assert observable behavior, durable outcomes, and authority boundaries rather than incidental internal calls or query shapes. Prefer typed builders and reusable stable fixtures over duplicated positional database-row tuples. Mock at focused boundaries; script exact SQL sequences or internal call ordering only when those details are material to the tested contract. Real-Postgres integration tests prove database-enforced constraints and atomicity. Do not rewrite existing tests solely for style.

When simulating the **full canonical Task repository-row projection** (the 18-column row consumed by `openorc.persistence.tasks` mapping), build rows with the shared builder in `tests/fakes/task_rows.py` instead of duplicating positional tuples. Specialized or partial-query row shapes keep bespoke local definitions, and Task persistence-mapping tests keep independent hand-written rows so column order and nullable handling stay independently verified. Detailed builder usage lives in the helper module itself, not here.

Normal test suites must not silently call live production GitHub, Supabase, Valkey, Cline Hub, or model inference. Real integration/E2E layers are explicit and separately configured.

Use `tests/README.md` for commands, suite layout, fixtures, markers, setup, and troubleshooting rather than bloating this standing policy file.
