# Domain Agent Context

## Boundary

The domain is the semantic heart of OpenOrc. It owns provider- and transport-independent entities, value objects, enums, validation, authority semantics, exact-subject invariants, and lifecycle coherence.

Do not import FastAPI, RQ, Supabase/GitHub/Cline SDKs, persistence, adapters, bridge mechanics, or Cloud code. Formal agent-response schema validation belongs to src/openorc/protocol/, not the domain.

## Adopted v1 model

Use the settled concepts rather than inventing parallel abstractions: Profile, Workspace, Project, Repository, Connection, WorkflowRoleBinding, GitHubInstallation, Task, TaskAgentSession, PlanRevision, ReviewLoop, ReviewIteration, OwnerGate, Execution, RuntimeRequest, TaskBlock, TaskPullRequest, and WorkflowEvent.

Do not add speculative generic entities, settings bags, workflow nodes, credential records, or replacement abstractions without a demonstrated product requirement.

## Ownership and identity

- Profile is the canonical OpenOrc application identity and reuses the corresponding Supabase Auth user UUID. Ordinary domain references point to Profile, never directly to auth.users.
- Ownership is Profile → Workspace → Project → Repository → Task. Workspace is the v1 security/configuration boundary.
- A Workspace has exactly one Owner Profile in v1. Membership, invitations, team RBAC, and organization abstractions are out of scope.
- Workspace configuration is typed. review_iteration_limit is positive and affects future ReviewLoops only; guidance is one current blank-by-default Owner-authored prose value. Guidance has no template/version/hash/history semantics and cannot redefine protocol, authority, session, or workflow behavior.
- Repository identity is its OpenOrc UUID plus the stable GitHub repository ID used for reconciliation. Mutable GitHub names, URLs, visibility, default branch, issue numbers, and similar presentation/address data are never sole identity.
- The same external GitHub repository may exist independently in different Workspaces; within one Workspace it is canonicalized by stable GitHub identity.

## Connections and role bindings

- Connection is a Workspace-scoped Agent Runtime route. It carries only safe Owner configuration, adapter type, enabled eligibility, Owner-configured session_capacity, and an opaque nullable auth_reference.
- enabled means OpenOrc may use the Connection; it is not runtime health or reachability.
- session_capacity is OpenOrc admission control, default 1, and is never discovered from the runtime.
- Raw credentials never appear in domain objects. Runtime-owned provider/MCP/tool credentials remain runtime-owned. OpenOrc-owned control-endpoint authentication is represented only by auth_reference.
- reported_provider, reported_model, and reported runtime metadata are nullable opaque observations, never configuration authority or enums.
- safe_config contains canonical non-secret JSON only.
- Exactly one WorkflowRoleBinding exists per (Workspace, role) in v1. Producer and Reviewer bindings may reference the same Connection or separate Connections. Runtime pools/failover are not v1 concepts.

## GitHub App installations and Repository routing (Phase 2B)

- A GitHubInstallation is the durable, Workspace-scoped record of one GitHub App installation available to a Workspace — a separate integration concept from Connection (agent-runtime route). A Workspace may hold several installations, and the same external installation may be represented independently in several Workspaces; Workspace isolation stays explicit.
- The record carries durable routing and observation facts only: stable external installation/account IDs, mutable observed account login/type, and the observed suspended_at. Raw GitHub App private keys, installation access tokens, PATs, and human OAuth tokens never appear in domain or persistence objects.
- Repository.github_installation_id is the explicit route to one installation record of the same Workspace: a durable configuration fact establishing which installation later GitHub operations must use — never an authorization claim, never inferred from mutable GitHub metadata. None means unconfigured: valid historical state that is not usable for GitHub operations until explicitly routed.
- No usability/authorization predicate is derived from observations anywhere in the domain: suspended_at is carried verbatim. Whether a routed installation currently grants access to a routed repository is later GitHub reconciliation work, never a property of the record or route.

## Task identity and lifecycle

- A Task is one governed attempt for one stable GitHub issue inside one Workspace-scoped Repository.
- At most one current/non-archived Task exists for a repository issue. Archived CANCELLED and COMPLETED attempts remain history; a fresh Task may follow cancellation or a later reopened completed issue.
- archived_at and terminal status are coherent: nonterminal Tasks are current; CANCELLED/COMPLETED Tasks are archived. Terminal transition goes through the archival operation.
- Task.status is the canonical coarse lifecycle state only: ready_to_plan, queued, planning, waiting_for_owner, implementing, reviewing, blocked, cancelled, completed.
- Do not encode gate type, review outcome, block reason, session health, CI, or runtime telemetry into Task.status. Those facts have their own canonical homes.
- state_token is an opaque optimistic-concurrency token. Every authoritative Task-state mutation rotates it; stale callers must fail without applying effects.
- current_plan_revision_id and current_owner_gate_id are pointers only. They never duplicate target content/outcomes. There is no singular current Execution pointer.
- The canonical feature branch is Task-level state. It starts unbound, is bound once after authoritative verification, remains exclusive among current Tasks in the Repository, and is reused by retries/remediation. Executions do not own branches.
- Ordinary lifecycle archives history. Explicit administrative purge is separate and never implies GitHub mutation.

## Task agent sessions

- Exactly one TaskAgentSession exists per (Task, role). Re-establishment is idempotent only when it agrees with the existing binding; never repoint or replace a binding silently.
- external_session_id is opaque. It is absent while CONNECTING and immutable after successful initialization.
- Lifecycle is CONNECTING → READY | ENDED and READY → LOST | ENDED. LOST and ENDED are absorbing.
- Recoverable runtime/Hub unavailability is not session loss. Genuine loss of the exact bound conversational context is LOST on the same binding and becomes a recovery/blocking concern.
- external_session_id, initialized_at, and effective_config_snapshot form one coherent initialization fact set. Before successful initialization they are all absent; afterward they are all present.
- effective_config_snapshot is caller-assembled, non-secret, immutable historical runtime/session configuration for that Task session. It is never a credential, prompt, schema, transcript, or Workspace-guidance snapshot.
- Producer and Reviewer sessions sharing one Connection each consume its capacity.

## Planning and review

- PlanRevision is immutable, versioned per Task, and stores the exact Producer plan plus repository_base_sha as audit/context. Base movement alone does not invalidate a cleared plan.
- ReviewLoop purposes are PLANNING and PR_REVIEW only. The loop stores the effective iteration limit used for that loop; later Workspace configuration changes do not rewrite it.
- ReviewIteration binds one exact subject and becomes immutable when its result is finalized.
- Reviewer outcomes are exactly ACCEPTED and CHANGES_REQUESTED. Operational/provider/protocol failure is not a Reviewer outcome.
- ACCEPTED has no findings. CHANGES_REQUESTED has at least one finding.
- Planning review binds a PlanRevision. PR review binds the canonical TaskPullRequest plus exact reviewed head SHA. A changed PR head invalidates acceptance; target/base movement alone does not.
- Review findings are durable historical evidence. The wire-envelope schema is protocol-owned and is not duplicated in domain state.
- ReviewLoop exhaustion leads to Owner REVIEW_RESOLUTION. Owner override remains a distinct authority fact and never rewrites CHANGES_REQUESTED into ACCEPTED.

## Human authority and subordinate records

- TaskPullRequest is the one canonical PR record for a Task in v1. A closed-unmerged PR is not replaced automatically. Mutable observed GitHub state updates the same record.
- OwnerGate types are IMPLEMENTATION_AUTHORIZATION, PR_AUTHORIZATION, MERGE_DECISION, REVIEW_RESOLUTION. Statuses are PENDING, APPROVED, REJECTED, CANCELLED.
- Gates bind exact subjects: implementation authorization → PlanRevision; PR authorization → exact committed Producer head; merge decision → TaskPullRequest + exact head; review resolution → exhausted PlanRevision or TaskPullRequest + exact head.
- Resolved gates are immutable and never recycled. Only one gate may be current at a time; resolution clears currency before another is installed.
- Execution is historical attempt data inside the Producer Task session. It is not a hidden workflow phase and does not imply one active execution globally.
- RuntimeRequest is a scoped Producer/runtime-originated ACTION_APPROVAL correlation. It is not free-form Owner ↔ Producer conversation.
- TaskBlock stores one durable block reason and recovery context. Resolving a block does not erase why it existed and does not imply one universal resume transition.
- WorkflowEvent is append-oriented audit history for consequential OpenOrc facts. It is not event sourcing, current workflow state, operational logging, or runtime telemetry.
- Logical WorkflowEvent actors are owner, openorc, producer, reviewer, runtime, github. Event context stays small and safe; never store raw credentials, request bodies, prompt/guidance prose, transcripts, or duplicated canonical records.

## Security and deletion invariants

- Workspace scope is explicit on Workspace-owned operational records and must agree with parent ownership.
- Missing, cross-Workspace, and cross-Task resources should not become an information oracle at service boundaries.
- Deleting OpenOrc state never deletes or mutates GitHub engineering artifacts or runtime-owned files/configuration/credentials.
- Runtime-control credentials are represented only through opaque references and secret-bearing values must remain outside ordinary domain objects.
- Permanent account deletion converges on hard Supabase Auth-user deletion after required OpenOrc-owned credential cleanup. Supabase soft deletion is not equivalent because the Auth row remains.
- Account deletion has a durable attempt identity/state used to prevent unsafe concurrent or blind replay. Recovery decisions bind to the exact attempt being reconciled.

Read the relevant services, persistence, protocol, adapter, transport, and Supabase guides for cross-boundary changes.
