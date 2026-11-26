# Notifications Agent Context

## Boundary

This package owns the shared internal PostgreSQL notification transport established by issue #397 (G4a): the fixed LISTEN/NOTIFY channel and identifier-only payload envelope contract, the API serving process's dedicated listener connection, and the bounded process-local delivery interface consumed by G4c (authorized Workspace SSE) and published to by G4b (notification-only database triggers).

It does not own database triggers or publication SQL (G4b), browser transport, SSE endpoints, per-client queues, or browser authorization (G4c), workflow semantics, authorization decisions, or any public DTO projection.

## Fixed transport contract

- One fixed channel: `openorc.notifications.contract.NOTIFICATION_CHANNEL`. Two typed message kinds discriminate through the envelope `kind` field: `workflow_event` (workspace_id, workflow_event_id) and `data_changed` (workspace_id, resource_type, resource_id). The resource vocabulary is closed: workspace, project, repository, issue, task, connection, role_binding, refresh; the workspace resource ID equals the workspace ID.
- Payloads are identifier-only: no raw rows, content, credentials, URLs, or provider payloads. The canonical encoding is compact JSON with the documented key order; G4b SQL must produce exactly that shape, and decoding accepts only the exact key sets with canonical lowercase hyphenated UUID strings.
- Strict validation, fail closed: `decode_notification_payload` rejects any malformed or unknown shape with `NotificationContractError`. Error messages carry shape facts and contract field names only — never payload-derived values — so they are safe to log.
- These notifications are internal hints: not authorization input, not workflow authority, not public DTOs. Downstream consumers own authorization and synchronization.

## Listener rules

- One dedicated autocommit psycopg 3 LISTEN connection per serving process, independent of the ordinary request pool; a pooled connection is never held across a subscription. The connection is owned exclusively by the reader thread (the `notifies()` generator holds the connection lock for its lifetime).
- LISTEN is session-bound. `resolve_db_listener_conninfo` refuses the known transaction-pooler port instead of silently subscribing through a mode that would never deliver, and requires an explicitly supplied listener DSN to carry the same database name as `DATABASE_URL` — a guard against demonstrable configuration mistakes, never proof of database identity. That both endpoints target the same PostgreSQL deployment and database is a documented operator configuration invariant (see docs/development.md); no introspection, identity records, or endpoint discovery may be added to enforce it.
- The listener is built and started by the API serving lifespan, exactly once per serving process in either execution mode — never at import, in `create_app()`, in an RQ parent or workhorse, or per browser connection. It is constructed and started in the OS process that serves it and is never inherited across a fork boundary.
- Readiness is authoritative and published only after LISTEN is active on the live connection. Connection loss and shutdown clear readiness and supersede pending readiness; a readiness signal is emitted only for a generation that is actually ready at delivery time.
- Delivery is generation-based: loss, overflow, and reconnect invalidate the old delivery generation and coalesce into typed loss signals requiring fresh downstream synchronization. There is no replay, cursor, sequence, notification persistence, or subscriber ledger; no automatic business retries; consumer failure is contained and never rolls back committed work or initiates Level 2/3 workflow handling.
- Bounded by construction: fixed-depth hand-off queue (the reader never blocks on it), O(1) coalesced control state (no control queue or per-generation backlog), bounded notify tick, bounded connect timeout, rate-bounded capped reconnect backoff, bounded join on clean stop. Keep these bounds fixed; do not add environment knobs.
- Consumers must be non-blocking and own their own resynchronization. No per-client queues or browser-owned state belong in this package (G4c's leaf adds those downstream).
- DSN values are credential material: never logged, never returned, never echoed in errors.

Read `src/openorc/api/AGENTS.md` for the lifespan ownership rule and `src/openorc/persistence/AGENTS.md` for the connection-separation rule.
