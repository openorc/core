# Domain Agent Context

## Boundary

The domain is the semantic heart of OpenOrc. It owns entities/value objects, state enums, transition rules, exact-subject invariants, authority semantics, and provider/transport-independent validation.

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
