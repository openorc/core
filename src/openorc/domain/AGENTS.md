# Domain Agent Context

## Boundary

The domain is the semantic heart of OpenOrc. It owns entities/value objects, state enums, transition rules, exact-subject invariants, authority semantics, and provider/transport-independent validation.

Do not import FastAPI, RQ, Supabase SDK, GitHub SDK, Cline SDK, bridge mechanics, or Cloud billing/deployment code.

## Adopted v1 concepts

Use the settled concepts rather than inventing parallel abstractions: Workspace, Project, Repository, Connection, workflow role/runtime binding, Task, TaskAgentSession, PromptTemplateOverride/effective prompt reference, PlanRevision, ReviewLoop, ReviewIteration, OwnerGate, Execution, RuntimeRequest, TaskBlock, TaskPullRequest, and WorkflowEvent.

Do not add speculative generic entities without a demonstrated v1 requirement.

## Ownership and repository identity (Phase 1)

- Profile is the canonical OpenOrc application identity. Its identifier is the corresponding Supabase Auth user UUID by value — a deliberate 1:1 infrastructure identity, not a foreign key into Supabase-managed schemas. Ordinary OpenOrc domain references point at Profile, never at `auth.users`.
- The ownership hierarchy is Profile → Workspace → Project/Repository. A Workspace is owned by exactly one Profile in v1; membership, invitation, and RBAC concepts do not exist.
- A Project belongs to one Workspace. A Repository belongs to one Project and carries its Workspace directly; the direct scope must agree with the owning Project's Workspace.
- Repository identity is the stable GitHub repository ID. Owner login, name, URL, visibility, and default branch are mutable observed metadata and never identity; changing them does not change which external repository a Repository record refers to. The same external GitHub repository may exist independently in multiple Workspaces; within one Workspace there is one canonical Repository record per GitHub repository identity.

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
