# Domain Agent Context

## Boundary

The domain is the semantic heart of OpenOrc. It owns entities/value objects, state enums, transition rules, exact-subject invariants, authority semantics, and provider/transport-independent validation of domain entities, value objects, and workflow invariants. Formal agent-response protocol-schema validation belongs to `src/openorc/protocol/`, not here; do not duplicate it in the domain.

Do not import FastAPI, RQ, Supabase SDK, GitHub SDK, Cline SDK, bridge mechanics, or Cloud billing/deployment code.

## Adopted v1 concepts

Use the settled concepts rather than inventing parallel abstractions: Workspace, Project, Repository, Connection, workflow role/runtime binding, Task, TaskAgentSession, PromptTemplateOverride/effective prompt reference, PlanRevision, ReviewLoop, ReviewIteration, OwnerGate, Execution, RuntimeRequest, TaskBlock, TaskPullRequest, and WorkflowEvent.

Do not add speculative generic entities without a demonstrated v1 requirement.

## Ownership and repository identity (Phase 1)

- Profile is the canonical OpenOrc application identity. Its identifier is the corresponding Supabase Auth user UUID by value — a deliberate 1:1 infrastructure identity, not a foreign key into Supabase-managed schemas. Ordinary OpenOrc domain references point at Profile, never at `auth.users`.
- The ownership hierarchy is Profile → Workspace → Project → Repository. A Workspace is owned by exactly one Profile in v1; membership, invitation, and RBAC concepts do not exist.
- A Project belongs to one Workspace. A Repository belongs to one Project and carries its Workspace directly; the direct scope must agree with the owning Project's Workspace.
- A Repository record's identity is its OpenOrc UUID. The GitHub repository ID is the stable external identity used for reconciliation and per-Workspace canonicalization; it survives renames and ownership transfers. Owner login, name, URL, visibility, and default branch are mutable observed metadata and never identity. The same external GitHub repository may exist independently in multiple Workspaces; within one Workspace there is one canonical Repository record per GitHub repository identity.

## Connections and workflow role bindings (Phase 1)

- A Connection is one configured agent-runtime route within one Workspace. It carries safe (non-secret) Owner configuration, adapter type, Owner-configured session capacity (admission control, default 1 — concurrency is explicitly enabled by the Owner), the Owner-controlled `enabled` eligibility switch, and the opaque nullable `auth_reference` boundary for OpenOrc-owned authentication. `enabled` never encodes runtime reachability/health; no speculative auth lifecycle/status state is persisted.
- Raw OpenOrc-owned credentials never appear in domain or persistence objects. Runtime-owned provider/MCP/tool credentials remain runtime-owned and are never modeled as OpenOrc credential records. OpenOrc-owned authentication is represented only through the opaque nullable `auth_reference`; validation/expiry/revocation belong to later functionality that can determine those facts.
- `reported_provider`/`reported_model` are nullable opaque runtime-reported observations — arbitrary strings or NULL, never enums, never configuration authority. A future OpenOrc-side selection capability adds separate `configured_*` concepts rather than repurposing these fields.
- A WorkflowRoleBinding is mutable per-role runtime configuration: exactly one binding per `(Workspace, role)` in v1 — no runtime pools, no failover. Producer and Reviewer bindings are independent and may reference the same Connection or separate Connections. The binding shape is exactly workspace/role/connection identity and timestamps; per-role session configuration is added only when a real configurable property exists (provider/model selection is intentionally unavailable; PLAN/ACT mode is workflow-derived; initialization prompts are OpenOrc-controlled protocol behavior).
- Capacity is Connection-scoped admission control configured by OpenOrc, never discovered from the runtime. TaskAgentSession occupancy against it is established by later session persistence work.
- `safe_config` is the non-secret Owner configuration container with canonical JSON-object semantics: string keys at every level, JSON-representable values only, sequences normalized to lists, and nothing relying on silent key coercion. It is validated for canonical form only, not credential safety; concrete adapter configuration schemas enforce allowed fields once those configurations exist. Durable checks mirror domain validation (nonblank name, NULL-or-nonblank auth reference, JSON-object safe_config) so a transaction cannot commit state the domain rejects afterwards.

## Task identity and lifecycle (Phase 1)

- A Task is one governed attempt to resolve one GitHub engineering issue. It belongs to one Workspace-scoped Repository and is backed by one stable GitHub issue identity (`github_issue_id`), which survives issue title/state changes. The repository-local issue number is observed address metadata, never identity; issue title/state are GitHub-owned presentation facts that are not stored on the Task.
- One GitHub issue has at most one current (non-archived) Task per Workspace Repository, enforced durably by a partial unique index. Archived attempts are unrestricted history, so a fresh Task can follow a CANCELLED attempt, and a previously completed issue may gain a fresh Task if GitHub later reopens it.
- Archival is represented independently from terminal outcome: `archived_at` records that an attempt is no longer current while `status` preserves which terminal outcome was reached. The settled invariant is archived ⟺ terminal: every `CANCELLED` or `COMPLETED` attempt is archived history and remains distinguishable, and every nonterminal attempt is current. Terminal transitions go exclusively through the archival operation, never through ordinary status updates.
- `Task.status` is the canonical coarse primary workflow state with exactly nine values: `ready_to_plan` (eligible leaf before autonomous work), `queued` (runtime-capacity backpressure, not failure), `planning`, `waiting_for_owner` (the primary state shared by every Owner-facing gate wait), `implementing`, `reviewing` (PR review/remediation), `blocked`, `cancelled`, `completed` (the only terminal outcomes). Status never absorbs subordinate facts that have their own canonical homes: Owner gate type/subject belong on OwnerGate (v1 gate types: IMPLEMENTATION_AUTHORIZATION, PR_AUTHORIZATION, MERGE_DECISION, REVIEW_RESOLUTION), review state/outcomes belong on ReviewLoop/ReviewIteration, and blocking reasons/context belong on TaskBlock. Do not introduce gate-specific, review-specific, or reason-specific Task statuses.
- The canonical feature branch is a Task-level fact: once later workflow/runtime logic verifies and binds the Producer-created branch, the branch identity is owned by the Task aggregate, never by individual Executions. A current Task's canonical branch is exclusive within its Repository (durably enforced); retries, later Executions, and PR remediation for that Task continue on the same branch rather than creating competing branch ownership. Binding is one-time: the branch is created NULL and bound exactly once, and a current Task never releases or switches its canonical branch. An archived attempt releases the branch, so a fresh Task after reopen may bind it again.
- `state_token` is the opaque optimistic-concurrency state token: every authoritative Task-state mutation replaces it, and mutations are conditional on the caller's expected token, so stale operations fail instead of mutating newer subjects. It is not history/revision numbering.
- `current_plan_revision_id`/`current_owner_gate_id` are nullable current-object pointers only. They identify related records and never duplicate those records' content or outcomes (one fact, one home). There is deliberately no singular current-Execution pointer: Executions are attempt/history records and multiple may exist.
- Ordinary lifecycle archives Tasks. Explicit Owner purge of archived internal Task data is a distinct, separately authorized operation and never implies GitHub mutation; nothing in the Task row is GitHub-authoritative.

## Task agent sessions (Phase 1)

- A TaskAgentSession is the durable Task/role ↔ external-session binding. Exactly one binding exists per `(Task, role)` in v1 for PRODUCER and REVIEWER. Establishment is idempotent for the same Connection and a deterministic conflict against a different one; a binding is never silently repointed, replaced, or duplicated.
- `external_session_id` is the opaque external-session identity. It is NULL while CONNECTING and, once successfully initialized, immutable for the binding's lifetime: no successor session ever replaces it. A non-null external session identity belongs to exactly one Task/role binding within its Connection; the same opaque identity may be bound independently under different Connections.
- Lifecycle vocabulary is CONNECTING, READY, LOST, ENDED. CONNECTING is establishment in progress and by definition not yet a bound session. Legal transitions are CONNECTING → READY | ENDED and READY → LOST | ENDED; LOST and ENDED are absorbing. `ended_at` is the semantic ENDED timestamp, set exactly when the status is ENDED and never substituted by `updated_at`.
- Runtime/Hub unavailability is not session loss and is not persisted lifecycle state: a recoverable Hub restart does not create a replacement binding. Genuine loss of the exact external session/context is LOST on the same binding; later workflow services translate that into `AGENT_SESSION_LOST` blocking behavior.
- `initialization_protocol_version` records the protocol version used to initialize the session (opaque string; absence is valid). `effective_config_snapshot` is the NON-SECRET effective runtime/session configuration snapshot captured at initialization: it is caller-assembled, never populated by blindly serializing Connection configuration or authentication material, and raw credentials/tokens must never enter it. Once a session has initialized, its snapshot is historical for that Task session; later Workspace role-binding/Connection configuration changes affect future sessions and never rewrite an initialized session's snapshot.
- `reported_provider`/`reported_model`/`reported_runtime_version` are nullable opaque runtime-reported provenance observations — arbitrary strings or NULL, never enums, never configuration authority.
- Capacity is Connection-scoped: Producer and Reviewer sessions sharing one Connection each consume occupancy against that Connection's Owner-configured `session_capacity`. Persistence exposes active-session accounting; admission decisions belong to later services.

## Core invariants

- One GitHub issue has at most one current non-archived Task; cancellation archives/releases that mapping for a fresh Task.
- Executable workflow runs on leaf Tasks; composite Tasks are tracking/orchestration containers.
- One executable Task has one Producer-created canonical branch and at most one canonical current PR.
- One Task/role keeps one persistent external session identity for the Task lifetime.
- Session creation is idempotent for `(Task, role)`; stage transitions, retries, Executions, and ReviewLoops do not create sessions.
- Session loss blocks/requires recovery; never silently replace a session.
- Producer and Reviewer working contexts remain separate even when sharing a physical runtime.

## Review and authority

- Reviewer outcomes are `ACCEPTED` and `CHANGES_REQUESTED`; provider/runtime/protocol failures are not reviewer judgments.
- Acceptance applies only to the exact review subject. PR acceptance is bound to exact head SHA; changed head invalidates it, base movement alone does not.
- Reviewer acceptance never authorizes implementation or merge.
- Owner override after exhausted review is a separate durable fact and never fabricates Reviewer acceptance.
- Free-form Owner ↔ Reviewer discussion never changes workflow state by itself.
- The exact cleared PlanRevision is what may be authorized for implementation.
- Once planning clears, v1 never returns to `PLANNING`; fundamental plan replacement means cancel/archive + fresh Task.
- Changed authoritative GitHub issue requirements after Task start block the attempt instead of being semantically merged.
- Stale delayed operations must not mutate newer workflow subjects or gates.

## Runtime requests and cancellation

- Consequential runtime approvals are scoped to the exact Task/session/request and resolved with typed controls, not general Owner ↔ Producer chat.
- Cancellation ends orchestration, may request runtime abort, does not resurrect on abort uncertainty, and does not implicitly mutate GitHub artifacts.

Domain changes affecting storage, API contracts, adapters, or UI require reading the corresponding local guides.
