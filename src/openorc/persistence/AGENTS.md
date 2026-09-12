# Persistence Agent Context

## Boundary

Persistence owns durable representation and repository/data-access mechanics for OpenOrc control-plane state. It does not define product workflow semantics merely because a table or constraint exists.

## Rules

- Postgres is the durable source for OpenOrc workflow/control state.
- Preserve Workspace scope explicitly on Workspace-owned operational data where practical.
- Enforce durable uniqueness/identity constraints that protect domain invariants, including at most one current/non-archived Task per repository issue.
- Store stable external identifiers and exact SHAs needed for reconciliation; mutable display fields must not be sole identity.
- Do not store runtime conversational context or chain-of-thought as workflow state.
- Raw secrets do not belong in ordinary application tables.
- Schema changes must remain aligned with `supabase/migrations/` and tests.
- Database shape must not become an excuse to bypass domain/service rules.

Read `supabase/AGENTS.md` for migration/auth/RLS concerns and domain/services guidance for semantic changes.
