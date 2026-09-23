# Persistence Agent Context

## Boundary

Persistence owns durable representation and explicit data-access mechanics for OpenOrc control-plane state. It enforces database invariants and exposes focused repository operations. It does not define product workflow policy merely because a table, constraint, or query exists.

## Core rules

- Postgres is the durable source for OpenOrc workflow/control state.
- OpenOrc application tables live in the openorc schema; Supabase-managed Auth remains in auth.
- Workspace-owned operational rows carry explicit workspace_id where the model requires direct scoping; durable constraints keep that scope consistent with parent ownership.
- Stable external IDs and exact commit SHAs are persisted where reconciliation needs them. Mutable display/address fields are never sole identity.
- Raw secrets, agent transcripts, private reasoning, runtime conversational context, and arbitrary request/response bodies do not belong in ordinary application tables.
- Persistence returns database/domain facts. Translation into application errors, workflow consequences, authorization, and retry policy belongs to services.
- Schema changes stay aligned with supabase/migrations/ and relevant tests.

## Driver, pools, and transactions

- Use synchronous psycopg 3 with psycopg_pool.ConnectionPool. Do not introduce a parallel async persistence stack.
- Pools are process-local and must be created in the OS process that uses them, never inherited across an RQ fork boundary.
- Pool access fails closed on PID change. Do not reuse, replace, or close a stale inherited pool.
- Pool bounds are per process; deployment sizing accounts for process count × pool size.
- Keep persistence pooler-neutral: prepare_threshold=None, no search_path dependence, no session-local GUC/state when transaction-scoped behavior suffices.
- READ COMMITTED is the normal isolation level.
- Transactions are short and composable. Repositories may use focused SELECT ... FOR UPDATE operations where semantics require a lock.
- No GitHub, Supabase Auth HTTP, Cline/runtime, model, or other external call occurs while a database transaction is open.
- Do not invent generic locking/query-builder abstractions when a focused repository operation expresses the invariant clearly.

## SQL and data conventions

- Use explicit parameterized SQL against fully-qualified openorc.* tables. Never interpolate values with f-strings.
- Use psycopg.sql only for genuinely dynamic identifiers.
- Internal records use UUID primary keys except deliberate 1:1 infrastructure identity such as Profile reusing the Supabase Auth UUID.
- Domain vocabularies use Python enums/value objects plus database text/CHECK constraints, not native Postgres ENUMs.
- Real-world instants use TIMESTAMPTZ. Python datetimes crossing this boundary are timezone-aware and normalized to UTC; reject naive datetimes.
- JSONB writes use explicit Jsonb adaptation and canonical JSON shapes defined by the domain boundary.

## Ownership and Workspace configuration

- profiles.id is the Supabase Auth user UUID. The sanctioned auth.users → profiles cascade is the account-root deletion relationship; ordinary domain foreign keys point to openorc.profiles.
- Repository identity is canonicalized by stable GitHub repository ID within a Workspace.
- Composite foreign keys are the standard durable backstop when a child stores both parent identity and workspace_id.
- Workspace settings are first-class typed columns, not a generic JSON settings bag.
- review_iteration_limit is positive and guidance defaults to blank text. Same-value configuration writes are no-ops and must not fabricate audit changes.
- Authorization policy never moves into SQL repositories merely because persistence can join the necessary rows.

## GitHub installation routing (issue #57)

- github_installations carries direct Workspace scope, stable external installation/account IDs, and mutable observed account/suspension facts; one canonical record exists per (Workspace, external installation ID), and the same external installation may exist independently in other Workspaces.
- Repository routing is a single nullable github_installation_id column: one route by construction, and the composite (github_installation_id, workspace_id) foreign key hooked on github_installations (id, workspace_id) makes cross-Workspace routing unrepresentable.
- The route FK is NO ACTION DEFERRABLE INITIALLY DEFERRED: hard installation deletion is blocked while a Repository routes to it (forced IMMEDIATE inside the delete transaction, mirroring the Connection-reference pattern), and unbinding the route is the explicit operation. Workspace aggregate deletion removes installation records in its ordered sequence.
- Reconciliation upserts observations (account ID as reported, login/type, suspended_at) and never rewrites record identity. No GitHub token, private key, PAT, or human OAuth credential is persisted.

## Connections and runtime-control credentials

- connections and workflow_role_bindings carry direct Workspace scope.
- Exactly one role binding exists per (Workspace, role) in v1.
- Connection.auth_reference is opaque and nullable. Raw credential material never appears in openorc.* tables.
- Connection.safe_config is non-secret canonical JSON only.
- enabled is Owner eligibility, not runtime health. session_capacity is Owner-configured admission control, not runtime-discovered state.
- Supabase Vault is the sole persistence boundary for OpenOrc-owned Agent Runtime control-endpoint credentials.
- persistence/runtime_control_secrets.py is the only module that queries vault.*. Other code must use its strict reference encode/parse and exact-ID operations rather than browsing or parsing Vault state itself.
- Existence checks do not decrypt. Decrypted values cross this boundary only for immediate wrapping by the trusted secret-bearing service/adapter boundary.
- Browser-facing roles have no Vault access. vault must remain outside exposed Data API schemas.

## Tasks and sessions

- At most one current Task exists per (Repository, stable GitHub issue ID); archived attempts remain unrestricted history.
- archived_at is coherent with terminal Task status. archive_task is the terminal mutation path; ordinary status updates are nonterminal.
- Every authoritative Task-state write is conditional on expected state_token and rotates it atomically.
- The canonical feature branch is nullable initially, bound once, and unique among current Tasks in a Repository.
- current_plan_revision_id and current_owner_gate_id are pointers only. Never duplicate target content/outcomes on Task and never add a singular current Execution pointer.
- Exactly one TaskAgentSession exists per (Task, role). Establishment never silently repoints an existing binding.
- Session initialization facts external_session_id, initialized_at, effective_config_snapshot move coherently. LOST/ENDED never create a successor binding.
- A non-null external_session_id is unique within its Connection.
- Active Connection occupancy is CONNECTING or READY; services own admission decisions.

## Planning, review, gates, attempts, requests, blocks, and PRs

- PlanRevisions are insert-only immutable history with per-Task revision numbering.
- ReviewLoops preserve their effective iteration limit. ReviewIterations bind one exact subject and become immutable when finalized.
- Planning-review subjects and PR-review subjects are structurally distinct. PR review binds TaskPullRequest + exact head SHA.
- Reviewer outcome/finding coherence is enforced durably: ACCEPTED has zero findings; CHANGES_REQUESTED has one or more.
- OwnerGates are one-shot immutable decisions after resolution. Their subject coherence must match gate type.
- Resolving a gate requires it still be pending/current and the Task state_token still match; stale attempts apply nothing.
- Executions are historical attempts anchored to the Producer session. Do not add a single-active-execution or current-execution persistence assumption.
- RuntimeRequest correlation identity is full-history within the Producer session; resolution/expiry/cancellation is one-shot.
- TaskBlocks retain reason/context after resolution; block resolution does not encode a universal next state.
- TaskPullRequest is unique per Task for the Task lifetime. Observed head/base/state/merge data updates in place; exact review identity lives on ReviewIteration, not on the mutable current head.
- Use only query-driven indexes and durable uniqueness constraints that serve demonstrated access/invariant needs.

## Workflow events

- WorkflowEvent persistence is INSERT-only append history. No ordinary update/upsert/rewrite path exists.
- Events are not event sourcing and never reconstruct canonical current Task state.
- task_id, when present, must agree with Workspace scope.
- Generic subject_type/subject_id is a both-or-neither pair for distinct modeled subjects; do not duplicate Task identity there when task_id already expresses the subject.
- Event context is small canonical JSON and never contains credentials, tokens, Workspace guidance, prompts/templates, transcripts, or duplicated canonical records.
- Read paths are bounded and deterministically ordered.

## Deletion ownership

- ON DELETE CASCADE is reserved for true ownership edges. Scope-consistency, cross-reference, historical-linkage, and upward current-object edges are restrictive/deferred where required. Do not add cascades mechanically.
- OpenOrc deletion never calls external systems and never mutates GitHub/runtime-owned artifacts.
- Aggregate deletion is explicit and ordered. Clear upward current pointers where required, remove owned descendants through the sanctioned ownership graph, then remove the aggregate root.
- Only archived Tasks are purgeable through the administrative purge path.
- Connection hard deletion is restrictive when historical/configuration references exist. Disconnect is the explicit revoke of enabled/auth_reference state; it is not equivalent to deleting history.
- The account root is auth.users. Permanent account deletion relies on hard Auth-user deletion after external cleanup; Supabase soft deletion leaves the OpenOrc graph in place.

## Administrative deletion concurrency

- Destructive administrative reads use explicit row locks so credential cleanup and aggregate deletion inspect the current protected state they mutate.
- Workspace deletion takes the Workspace root lock before enumerating/deleting children, preventing concurrent child insertion from entering the aggregate mid-delete.
- Account deletion takes the Profile root lock for the claim/cleanup transaction.
- The account-deletion attempt state is a coherent tuple: either no attempt state exists, or state + exact attempt UUID + establishment timestamp all exist.
- Attempt-state writes are compare-and-swap against the exact attempt they own. One invocation must never clear or advance another attempt.
- Lease comparisons use the database clock.
- Owner-mutation barrier reads use FOR KEY SHARE so ordinary guarded mutations serialize correctly against the account-deletion FOR UPDATE claim while remaining compatible with one another.

Read supabase/AGENTS.md for migration/Auth/Vault deployment rules and domain/services guidance for semantic changes.
