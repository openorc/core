# Domain Agent Context

## Boundary

The domain is the semantic heart of OpenOrc. It owns entities/value objects, state enums, transition rules, exact-subject invariants, authority semantics, and provider/transport-independent validation.

Do not import FastAPI, RQ, Supabase SDK, GitHub SDK, Cline SDK, bridge mechanics, or Cloud billing/deployment code.

## Adopted v1 concepts

Use the settled concepts rather than inventing parallel abstractions: Workspace, Project, Repository, Connection, workflow role/runtime binding, Task, TaskAgentSession, PromptTemplateOverride/effective prompt reference, PlanRevision, ReviewLoop, ReviewIteration, OwnerGate, Execution, RuntimeRequest, TaskBlock, TaskPullRequest, and WorkflowEvent.

Do not add speculative generic entities without a demonstrated v1 requirement.

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
