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

## v1 driver and connection conventions

These are durable Phase 1 conventions. Later issues inherit them; do not re-litigate them issue-by-issue. Changing one is a deliberate architecture decision, not an implementation detail.

- Postgres is accessed through synchronous psycopg 3 with `psycopg_pool.ConnectionPool`. No async psycopg API and no parallel sync/async persistence implementations exist at this boundary. The synchronous foundation serves API/service code and RQ workers alike.
- Pool ownership is strictly process-local. A pool belongs to exactly one OS process, and the `ConnectionPool` object must be created in the OS process that uses it, after any fork that process has gone through. Never construct a pool anywhere it could be inherited across an RQ fork boundary (the default RQ worker forks a child process per job): an RQ parent must not establish database pool/connection state before forking its work horse. Process surfaces own the lifespan/ownership wiring that satisfies this invariant within their process.
- Pool access is process-aware: the accessor tracks the owning PID and fails closed on PID change. A changed process identity can never reuse, replace, or close the previous pool; a stale fork-inherited pool is abandoned untouched. A process creates a pool through the explicit fresh-pool path only when its parent established no pool before the fork.
- Pool size bounds are per process, not deployment-wide. Deployment sizing must account for process count × pool size.
- Connections are pooler-neutral by default: `prepare_threshold=None`, no dependence on `search_path`, and no session-local state (no session `SET`/GUCs). Pooler mode (direct/session/transaction) is a deployment concern and must never leak into persistence code or domain meaning.
- Transactions are thin and composable: yield one connection and delegate commit-on-success, rollback-on-error, and connection-return to psycopg/psycopg_pool connection-context semantics. OpenOrc does not duplicate transaction machinery, and repositories do not establish auto-commit behavior.
- No external call (GitHub, Cline, model inference, or any other external system) may occur while a database transaction is open.
- Row locking is narrow and deliberate: repositories author semantic `SELECT ... FOR UPDATE` operations inside short transactions where the domain requires them. There is no generic lock-row or query-builder locking abstraction.
- `READ COMMITTED` is the normal isolation level; the persistence foundation does not customize isolation.

## SQL and data conventions

- Repositories are explicit SQL repositories. Table references use fully qualified `openorc.*` names, values are always parameterized, and `psycopg.sql` composition is used only where dynamic identifiers are genuinely required. No f-string SQL.
- OpenOrc-owned application tables live in the dedicated `openorc` schema. Migration authority, grant posture, and the append-only migration rule are owned by `supabase/AGENTS.md`; persistence code never mutates committed migration history.
- Internal domain records use UUID primary keys unless an explicit 1:1 infrastructure identity deliberately reuses another system's UUID (e.g. `Profile` mapping to the Supabase Auth user UUID).
- Workflow/type vocabularies are Python enums/value objects enforced in the database as text columns plus CHECK constraints, never native Postgres ENUMs.
- Instants are stored as `TIMESTAMPTZ`. Python datetimes crossing the persistence boundary are timezone-aware and normalized to UTC; naive datetimes are rejected. `utc_now()`-style helpers are low-level utilities, not workflow-clock semantics.

Read `supabase/AGENTS.md` for migration/auth/RLS concerns and domain/services guidance for semantic changes.
