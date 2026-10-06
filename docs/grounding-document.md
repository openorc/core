# Open Orchestrator (OpenOrc)
## Grounding Document

**Status:** Evolving canonical design source before and during early implementation  
**Product name:** Open Orchestrator  
**Short name:** OpenOrc  
**Reference deployment:** Babelbeez

---

# 1. Purpose and Authority

This document is OpenOrc's evolving design source of truth. It describes the product and architecture as they are currently intended to work.

It is not a decision log, meeting record, or historical specification. When a design decision changes, this document changes with it. Superseded approaches do not remain merely to preserve history; Git already does that job perfectly well and with considerably less prose.

The document exists to keep product semantics, architectural boundaries, workflow contracts, and unresolved implementation questions coherent until they have more natural repository-native homes.

The intended knowledge transition is:

```text
Grounding Document
        ↓
GitHub issues / sub-issues / dependencies
        +
optional Project / milestone views where useful
        +
README.md
        +
root AGENTS.md
        +
nested AGENTS.md files near relevant code
        ↓
implementation
        ↓
repository code / schema / configuration increasingly becomes authoritative
```

During early implementation, this document remains the canonical product/design reference when repository artifacts have not yet captured the same knowledge. As implementation matures, durable guidance moves into the repository and this document becomes progressively less important.

The structured `AGENTS.md` hierarchy should preserve implementation guidance according to where it matters: global product and architectural invariants at the root, and domain-, frontend-, backend-, persistence-, adapter-, and deployment-specific constraints in nested files.

The governing rule is:

> Maintain one current design truth. Do not preserve superseded designs or maintain competing specifications indefinitely.

---

# 2. Product Thesis and Positioning

OpenOrc is an open control plane for governed agentic software development.

Its purpose is to keep the engineering workflow stable while models, inference providers, coding agents, Reviewer systems, and execution runtimes remain replaceable. OpenOrc v1 is deliberately Cline-first: it is built first for experienced Cline users who want to delegate substantial engineering work while adding independent review, explicit human authority, and durable GitHub-backed workflow state around Cline Hub. Runtime neutrality remains an architectural boundary, not a v1 adapter-breadth objective.

The core principle is:

> The engineering workflow should remain stable even when the model stack is disposable.

Models and runtimes are workers. The workflow, authority boundaries, engineering record, and human accountability are the durable system.

OpenOrc sits above and between existing engineering systems where deterministic workflow coordination, durable state, independent review, explicit authority, and accountability are required.

It is not intended to replace:

- coding-agent runtimes such as Cline, Codex, or Claude Code;
- frontier models or inference routers;
- GitHub;
- CI systems;
- observability, analytics, security, SEO, dependency, or other signal-producing systems.

The durable product identity is the governed engineering control layer:

- GitHub-backed Tasks;
- explicit reviewed intent;
- deterministic authority boundaries;
- scoped runtime action-approval requests;
- provider-neutral adapter-backed Agent Runtime Connections;
- independent review against exact review subjects;
- durable workflow/audit history;
- controlled progression from engineering work to merged repository outcome;
- later, a governed path from external engineering signals into the same workflow.

OpenOrc should not become a generic no-code workflow builder or a general-purpose multi-agent framework. Its primitives retain engineering meaning: Tasks, PlanRevisions, Reviews, OwnerGates, Executions, RuntimeRequests, Connections, PRs, and workflow events.

## 2.1 Human attention is the scarce resource

OpenOrc optimizes for useful autonomous activity per unit of human attention, not for maximum agent activity.

Automation should remove mechanical coordination such as:

- moving context between systems;
- remembering workflow state;
- routing Producer and Reviewer messages;
- handling retries and runtime waits;
- surfacing runtime requests that require an Owner decision;
- synchronizing GitHub state;
- monitoring for conditions that require human attention.

Humans remain responsible for:

- intent;
- judgment;
- authorization;
- risk acceptance;
- Workspace-required product validation;
- merge decisions;
- accountability.

The preferred operating principle is:

> Maximum automation between gates, explicit human authority at consequential boundaries.

## 2.2 Provider neutrality and economics

Different tasks may use different models, providers, runtimes, or services according to quality, cost, speed, latency, availability, privacy, context capacity, and task type.

A logical capability may be reached through a direct model endpoint, inference router, agent runtime, webhook, MCP connection, HTTP API, or another future transport. Workflow semantics must not depend on a vendor name or transport mechanism.

OpenOrc should support user-selected providers and BYO authentication where OpenOrc is the direct consumer. Externally managed runtimes retain ownership of their provider/tool credentials. OpenOrc Cloud-managed runtimes may use Workspace-scoped managed secret material only where required to reproduce the managed runtime through validated supported surfaces; the deliberate exception is the Owner's Profile-scoped GitHub App user authorization, which remains one human authorization across that Profile's Workspaces and may be consumed by Cloud for managed-runtime Git access.

## 2.3 Babelbeez reference deployment

Babelbeez is the first dogfood environment and reference deployment. It proves the workflow and supplies concrete infrastructure choices for v1.

Babelbeez-specific repositories, validation practices, credentials, deployment details, or runtime choices are not universal OpenOrc product requirements unless this document explicitly says otherwise.

> Babelbeez is the proving ground. OpenOrc is the product.

---

# 3. Product Principles

## 3.1 Independent review

Producer and Reviewer are separate logical roles. OpenOrc must preserve the ability to bind them to different models, providers, runtimes, or services. Logical independence does not require separate physical runtime infrastructure: v1 may bind both roles to the same Cline Hub or to separate Hubs, provided they use separate Task-scoped sessions, separate working contexts, and role-specific instructions/policies. The OpenOrc core does not prescribe the physical Hub topology.

```text
Producer proposes
↓
Independent Reviewer evaluates
↓
Human authorizes consequential progression
```

Independent review is intended to reduce correlated failure, not manufacture disagreement.

## 3.2 Human authority and accountability

Technical readiness is not implementation authorization, and implementation authorization is not blanket permission for every consequential runtime action.

Agents may perform work. Humans remain accountable for consequential decisions.

Logical actor identity must remain distinct from infrastructure credential identity. Shared Git or service credentials must not erase who or what logically produced, reviewed, authorized, executed, or merged an action.

Durable provenance should make it possible to determine, where applicable:

- what requirement or engineering signal caused work to begin;
- which Producer/runtime/model produced a plan or implementation result;
- which Reviewer evaluated which exact subject;
- whether technical review passed;
- which human authorized lifecycle transitions or sensitive actions;
- what was committed, pushed, reviewed, and merged.

## 3.3 GitHub remains the durable engineering record

GitHub is the durable shared engineering record for:

- issues and requirements;
- issue hierarchy and dependencies;
- the published review-cleared plan;
- branches and commits;
- pull requests;
- CI/check state;
- branch protection and merge policy;
- durable merge outcomes and engineering milestones.

Intermediate Producer ↔ Reviewer negotiation, runtime-private state, and conversational context do not become GitHub noise.

GitHub actor attribution follows human accountability. GitHub mutations that create or change the durable engineering record on the Owner's behalf are performed through the OpenOrc GitHub App's user-to-server authorization for that human, so the human remains the GitHub actor of record. OpenOrc and its Producer, Reviewer, or future specialist agent roles are not separate GitHub engineering actors. OpenOrc's own durable audit state preserves the internal provenance of what agents produced or reviewed and what the human authorized.

The same GitHub repository may be used by multiple humans through independent OpenOrc accounts/Workspaces. They may share the same physical GitHub App installation on the repository-owning account or organization while each human maintains a separate Profile-scoped GitHub user authorization and remains accountable for their own GitHub-attributed actions.

## 3.4 Task boundaries are conversation boundaries

Each executable Task has one persistent session per configured agent role for the lifetime of that Task.

When a Task begins its workflow, OpenOrc creates a fresh isolated session for each configured role. Later planning iterations, owner gates, implementation attempts, retries, PR remediation, and review iterations continue in those exact sessions.

> New Task = new role sessions. Workflow transitions inside a Task = the same role sessions.

The connected runtime/provider owns conversational context. OpenOrc owns lifecycle orchestration and the routing binding to the exact external session.

## 3.5 Communication topology is constrained

Conversational communication is:

```text
Owner ↔ Reviewer ↔ Producer
```

More precisely:

- Owner ↔ Reviewer free-form discussion is allowed at Reviewer-related owner decision points;
- Reviewer ↔ Producer communication occurs through OpenOrc-managed ReviewLoops;
- Owner ↔ Producer free-form conversation is not allowed.

Owner actions affecting the Producer travel as typed OpenOrc workflow controls such as authorization, remediation requests, cancellation, or scoped responses to Producer/runtime-originated action-approval requests. They are not Owner-initiated free-form chat messages.

Free-form discussion is advisory and does not directly change workflow state.

## 3.6 OpenOrc owns protocol and workflow controls; users configure optional guidance

OpenOrc separates the following interaction concerns:

1. **Formal JSON schemas** are canonical OpenOrc-owned `.json` machine contracts. They are non-overridable and carry explicit `schema_version` because structural compatibility matters.
2. **Role prompts** are runtime-neutral Producer/Reviewer Markdown: OpenOrc ships defaults and the Owner may configure an optional per-role override. They configure role behavior without redefining protocol or authority.
3. **Role initialization prompts** are canonical OpenOrc-owned Producer/Reviewer `.md` artifacts. They are non-overridable and may render the canonical schemas into controlled insertion points rather than duplicating schema definitions.
4. **Workflow controls** are OpenOrc-owned semantic instructions such as plan, review, implement, remediate, or compose PR content. They are non-overridable. A runtime adapter may realize a control through a terse prose message, a runtime-native action, or both.
5. **Workspace guidance** is optional Owner-authored prose, blank by default, separate from role prompts. OpenOrc may inject it into controlled initialization or interaction locations where useful. Guidance may influence emphasis or working conventions but cannot redefine protocol, authority, communication topology, session boundaries, exact review-subject identity, or workflow transitions.

OpenOrc audits resulting workflow facts and authority decisions, not the exact historical prose sent to an agent. It does not persist prompt/template versions, hashes, snapshots, or per-interaction prompt-use records. Formal machine contracts retain `schema_version` because incompatible schema changes can break parsing/validation.

Repository-owned context, including root `AGENTS.md`, and the runtime-native system/mode/tool harness are independent layers. OpenOrc does not hydrate native rule files.

> OpenOrc owns protocol, workflow controls, and default role prompts. Owners may customize role prompts and provide optional Workspace guidance within those authority boundaries. Agents provide semantic results.

## 3.7 Persistent Workspace development environments

OpenOrc distinguishes the long-lived development environment from the Task-scoped agent sessions that execute inside it.

For a Workspace that uses the OpenOrc Cloud-managed runtime option, the reference topology is one persistent managed development environment for that Workspace. It may contain multiple Workspace repositories, development dependencies and toolchains, caches, CLIs, runtime-native configuration, and other Workspace-local engineering state. The environment is hydrated and reused across Tasks rather than recreated as a disposable per-Task sandbox.

Each Task still receives fresh isolated Producer and Reviewer sessions and working contexts. Multiple Tasks may execute concurrently inside the same Workspace environment when configured runtime capacity permits. Workspace-environment persistence and Task-session persistence serve different purposes; neither becomes workflow authority or durable engineering truth.

In v1, the OpenOrc-managed implementation is the ClineBox: a persistent Workspace development environment running Cline as its Agent Runtime. The managed development environment is the hosted capability; Cline remains the replaceable runtime boundary.

## 3.8 Open source and self-hosting

OpenOrc core is intended to be genuine open-source software under Apache License 2.0.

Self-hosted OpenOrc and OpenOrc Cloud use the same core product. Hosted value comes from operating the system well, not from crippling self-hosting or forcing bundled providers.

---

# 4. v1 Scope

OpenOrc v1 implements the issue-to-PR lifecycle for independently executable GitHub issues. Larger work is represented through GitHub-native sub-issues and dependencies rather than OpenOrc-private implementation phases.

Canonical Task flow:

```text
GitHub Issue
↓
OpenOrc Task
↓
Producer prepares implementation strategy
↓
Producer ↔ Reviewer bounded planning loop
↓
Human implementation authorization
↓
Producer implements
↓
Scoped runtime action approval where required
↓
Human PR authorization
↓
Producer composes PR title/body
↓
OpenOrc creates Pull Request on behalf of Owner
↓
Producer ↔ Reviewer PR remediation loop over committed Git state
↓
Human merge decision
↓
Completed
```

GitHub hierarchy may organize related work without determining executability:

```text
GitHub parent issue → may have its own OpenOrc Task when otherwise eligible
├── sub-issue → may have its own OpenOrc Task when otherwise eligible
├── sub-issue → may have its own OpenOrc Task when otherwise eligible
└── sub-issue → may have its own OpenOrc Task when otherwise eligible
```

Parent/sub-issue relationships are descriptive hierarchy only. GitHub issue dependencies determine blocking and sequencing where required.

Planning that establishes product intent or creates the GitHub issue is outside v1. Producer planning is execution preparation for an already-defined engineering Task.

## 4.1 v1 capabilities

v1 includes:

- first-class Workspace isolation;
- Workspace-scoped Agent Runtime Connections and their OpenOrc-owned control-endpoint authentication/configuration;
- OpenOrc-owned formal JSON schemas, role initialization prompts, and workflow controls, plus optional blank-by-default Workspace guidance;
- GitHub issue intake with at most one current/non-archived Task per issue and retained historical archived Task attempts;
- GitHub parent/sub-issue and dependency synchronization;
- GitHub webhook intake with authoritative-state reconciliation;
- GitHub hierarchy projection for view/inform UX without inferring executability or blocking from parent/sub-issue shape;
- GitHub-authoritative issue dependency blocking used for Task intake and sequencing;
- one persistent Producer session and one persistent Reviewer session per executable Task;
- parallel executable Tasks where the configured role bindings' underlying runtime Connection(s) have sufficient aggregate isolated-session capacity;
- one independently configured runtime binding per role in v1; each binding references one Agent Runtime Connection, and both roles may reference the same Connection while retaining separate Task sessions and role-specific session configuration;
- mandatory OpenOrc initialization and validated `session_ready` v1 handshake before a role session becomes usable;
- stable Task/role ↔ external-session routing for the Task lifetime;
- Producer implementation strategies as versioned PlanRevisions;
- bounded independent planning review with a default maximum of five Reviewer evaluations, configurable per Workspace;
- Reviewer outcomes `ACCEPTED` and `CHANGES_REQUESTED`;
- operational/provider/protocol failures through a separate adapter error path;
- owner intervention through `REVIEW_RESOLUTION` after ReviewLoop iteration-limit exhaustion, with Discuss / explicit Owner override / Cancel;
- publication of the final review-cleared plan to GitHub;
- implementation authorization tied to the exact review-cleared PlanRevision;
- in-UI Owner ↔ Reviewer discussion at Reviewer-related owner gates;
- Producer-only implementation;
- execution attempt/history tracking without replacing the Task's Producer session;
- runtime retry and recovery, including adapter-requested abort on Task cancellation;
- Owner-accessible runtime activity cancel/resume controls where the bound Agent Runtime exposes supported controls, operating on the same TaskAgentSession without cancelling the OpenOrc Task or replacing/resetting its external session;
- scoped Producer/runtime-originated action-approval requests for consequential runtime actions;
- human PR authorization without prescribing a universal validation method;
- Producer-authored PR title/body through validated `pr_result`;
- canonical PR creation by OpenOrc through GitHub API on behalf of the accountable Owner;
- commit-addressed independent PR review using the existing Reviewer session;
- Producer remediation and re-review using the existing Task sessions;
- display of GitHub CI/check state without duplicating GitHub merge policy;
- human merge decision;
- durable workflow/audit events;
- browser-facing Server-Sent Events (SSE) for live OpenOrc event delivery;
- optional adapter-exposed runtime telemetry for live agent status/activity where a runtime supports it;
- optional adapter-exposed links to native external runtime/provider interfaces where useful;
- dashboard-centered Task wizard UX with one active wizard per tab;
- operator workflow/configuration UI;
- `ClineAdapter` as the sole v1 Agent Runtime adapter, usable independently by both Producer and Reviewer roles;
- role-specific runtime session configuration; Cline v1 uses opaque Owner-entered provider/model identifiers and runtime-neutral role Markdown, realized through the qualified public SDK while native authentication/configuration remains Cline-owned;
- OpenOrc Cloud-managed persistent Workspace development environment (ClineBox) lifecycle as a hosted/reference capability, with Workspace-scoped hydration reused across Tasks, guarded hibernation/snapshot restore, and reproducible clean rebuild, without making runtime provisioning or hibernation a universal Core Agent Runtime requirement;
- fake/test Agent Runtime adapters and role behaviors for deterministic workflow testing;
- GitHub-only user sign-in through Supabase Auth;
- GitHub App installation authorization for repository routing, infrastructure reads, reconciliation, webhook recovery, and capability observation, with Workspace/repository routing bound to the relevant installation rather than to an Agent Runtime Connection;
- Profile-scoped GitHub App user-to-server authorization for Owner-accountable mutations that create or change the durable GitHub engineering record, bound to the same stable GitHub human identity as the Profile's GitHub sign-in.

## 4.2 v1 non-goals

v1 does not include:

- signal ingestion from email, PostHog, Ahrefs, vendors, monitoring systems, or similar sources;
- automatic merge;
- automatic production deployment;
- automatic replacement of Workspace-defined human validation;
- OpenOrc-generated implementation phases beneath a GitHub issue;
- `LiveExecutionPlan`, `ExecutionStep`, `ActiveTask`, or equivalent private phase machinery;
- repository-local `activeTask.md` as an OpenOrc primitive;
- independent Reviewer loops over uncommitted implementation state;
- semantic plan-drift detection by OpenOrc during implementation;
- OpenOrc ownership or reconstruction of agent conversational context;
- cross-Task reuse of Producer or Reviewer sessions;
- a generic Connection capability registry, universal adapter SDK, provider marketplace, or transport framework;
- implicit session reset/replacement at workflow-stage, retry, Execution, or ReviewLoop boundaries;
- a free-form Owner ↔ Producer chat channel;
- natural-language inference of workflow transitions from discussion text;
- adopting an already-open PR as a new OpenOrc Task;
- multiple configured agent runtimes per role, runtime pools, pool scheduling, or runtime failover;
- horizontal worker fleets;
- Kubernetes or a dedicated durable orchestration framework such as Temporal;
- custom coding-agent runtimes or custom model/tool execution loops;
- multi-user SaaS tenancy beyond the initial account/Workspace model;
- organization/team administration, invitations, or complex RBAC;
- dedicated OpenOrc Valkey infrastructure;
- persistent filesystem state in API or worker services;
- a generic visual workflow builder or general-purpose agent framework.

Signal intake remains part of the long-term product thesis but is deferred until the issue-to-PR workflow is reliable.

---

# 5. Architecture and Sources of Truth

The canonical architecture is:

```text
                         GITHUB
      durable engineering record + authoritative decomposition
                         ⇅ API

                 OPENORC CONTROL PLANE
                 FastAPI + Vue + RQ
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ↓                ↓                ↓
   Supabase           Valkey        Connection / Agent Runtime
 Postgres/Auth                           Adapter Layer
                                             │
                                        ClineAdapter
                                             │
                                  Cline runtime Connection(s)
                                      /              \
                         Producer Task sessions   Reviewer Task sessions
                         role/runtime config      role/runtime config
```

Cline is the first Agent Runtime for both Producer and Reviewer roles. The qualified managed-Cline direction is one persistent managed Cline Hub/Box to which OpenOrc attaches as a published `@cline/sdk` `ClineCore` remote client. Producer and Reviewer remain distinct Task-scoped role sessions with isolated working contexts even when they share that Hub. OpenOrc selects provider/model per role/session; Cline owns provider authentication and native runtime configuration. Other Agent Runtimes may be added later behind the same semantic session boundary without changing workflow semantics.

## 5.1 GitHub owns

GitHub is authoritative for:

- issue identity and requirements;
- parent/sub-issue hierarchy;
- issue dependencies;
- the durable published copy of the review-cleared plan;
- branches and commits;
- pull requests;
- CI/check state;
- branch protection and merge policy;
- merge result;
- durable engineering milestones where useful.

GitHub is not a command transport for Producer authority. Comments, labels, and audit artifacts do not implicitly authorize runtime actions.

## 5.2 OpenOrc owns

OpenOrc owns:

- GitHub issue ↔ Task mapping;
- mirrored GitHub hierarchy/dependency state needed for orchestration and UI;
- Task workflow state;
- PlanRevisions and ReviewLoops;
- OwnerGates and scoped runtime `RuntimeRequest`s;
- Task/role session lifecycle orchestration and opaque external-session bindings;
- deterministic routing to the exact Task-scoped role session;
- execution attempt/history metadata;
- deterministic retry/block/recovery policy;
- Connection definitions and workflow role bindings;
- OpenOrc-owned formal JSON schemas, role prompts and role initialization prompts;
- OpenOrc-owned workflow controls and optional Workspace guidance;
- non-editable protocol envelopes and structured result contracts;
- Owner ↔ Reviewer discussion routing;
- GitHub workflow integration;
- durable workflow/audit events;
- operator-facing workflow and configuration UI;
- authentication/configuration for external services OpenOrc directly consumes;
- Workspace-scoped managed-runtime configuration and secure secret references where OpenOrc Cloud is responsible for reproducing that runtime.

OpenOrc does not own runtime-private reasoning, conversational history, or uncommitted filesystem state. OpenOrc-managed runtime configuration/secret ownership does not make runtime-private conversational or execution state durable OpenOrc state.

## 5.3 Connected runtimes/providers own

Connected agent systems own the conversational context of the sessions they expose to OpenOrc.

An Agent Runtime such as Cline also owns execution concerns including:

- model calls;
- shell execution;
- filesystem operations;
- repository editing;
- tools and MCP execution;
- runtime-specific persistence;
- runtime-private working state;
- for externally managed runtimes, runtime-owned provider, MCP, CLI, tool, and account credentials.

OpenOrc integrates through adapters rather than reimplementing those responsibilities. For the managed Cline Box, Cline remains owner of provider authentication and native runtime configuration even though OpenOrc Cloud owns the host lifecycle. Cloud may retain infrastructure credentials and OpenOrc-owned control-endpoint authentication required to provision and reach the Box, but it does not duplicate Cline-native provider, MCP, plugin, tool, sub-agent/team, or other runtime configuration merely to make the host managed. A retained hibernation snapshot is infrastructure state, not OpenOrc workflow authority or conversational history. Clean rebuild may require the Owner to re-establish Cline-native configuration through supported Cline surfaces rather than OpenOrc parsing or reproducing private runtime files.

## 5.4 Persistence ownership

Supabase/Postgres owns durable OpenOrc control-plane state, including:

- Workspaces, Projects, Repositories;
- Task mappings and mirrored GitHub relationships;
- Task workflow state;
- TaskAgentSession bindings and lifecycle/routing metadata;
- PlanRevisions;
- ReviewLoops and ReviewIterations;
- structured Reviewer outcomes;
- OwnerGates;
- Executions;
- RuntimeRequests;
- TaskBlocks;
- TaskPullRequest mappings/cache metadata;
- workflow/audit events;
- Agent Runtime Connections and role bindings;
- current optional Workspace guidance;
- non-secret control-plane configuration;
- references/status for OpenOrc-owned authentication.

OpenOrc secure authentication/deployment boundaries own raw authentication material that OpenOrc itself must hold. GitHub App installation access tokens and Profile-scoped user access tokens are short-lived and minted/refreshed as needed rather than persisted as ordinary application credentials. The durable per-human refresh credential required for GitHub App user-to-server authorization is an OpenOrc-owned secret stored through the supported encrypted secret boundary (Supabase Vault in v1) behind an opaque Profile-scoped reference; raw token values never live in ordinary `openorc.*` columns, domain DTOs, events, logs, telemetry, or browser-readable state.

The deployment environment owns bootstrap secrets required to start OpenOrc and reach its infrastructure.

The review-cleared PlanRevision has a deliberate dual representation: OpenOrc's immutable PlanRevision is the workflow-authority artifact tied to implementation authorization; GitHub stores the durable published copy for the engineering record. Editing the GitHub copy does not silently alter the OpenOrc PlanRevision. Once planning has been cleared, changing the plan requires cancellation/archive of the current Task and a fresh Task rather than returning the existing Task to `PLANNING`.

The concise rule is:

> GitHub stores durable engineering truth. OpenOrc stores deterministic workflow/control truth and routing bindings. Connected runtimes/providers store conversational context; Agent Runtimes also store private execution state and credentials for services they directly consume.

---

# 6. GitHub Task Model and Access Boundary

## 6.1 One current OpenOrc Task per GitHub issue

A Task is backed by one GitHub issue in one Workspace-scoped Repository. One GitHub issue may accumulate historical OpenOrc Task attempts, but it has at most one current/non-archived OpenOrc Task at a time inside that Workspace.

```text
GitHub issue #123
├── historical Task A — COMPLETED / archived
├── historical Task B — CANCELLED / archived
└── current Task C
```

Archival and terminal outcome are distinct facts. `archived_at` means an OpenOrc Task attempt is no longer current; the Task's terminal workflow status records whether that attempt ended `CANCELLED` or `COMPLETED`.

Cancelling a Task ends and archives that OpenOrc attempt and releases an open GitHub issue so a fresh Task may be started against the issue's then-current authoritative state. A completed Task is likewise archived after GitHub confirms merge; the corresponding merged PR normally closes the GitHub issue. If that completed GitHub issue is later reopened, the reopened issue represents renewed engineering intent and may receive a fresh OpenOrc Task with fresh Producer and Reviewer sessions. Historical Task attempts are never revived or reused as the new attempt.

The persistence model must enforce the equivalent of uniqueness for the current Task mapping using stable GitHub issue identity:

```text
(repository_id, github_issue_id)
→ at most one current/non-archived Task
```

A GitHub repository is not globally claimed by one OpenOrc Owner or Workspace. The same external repository may be connected independently in multiple Workspaces; within a Workspace there is one canonical Repository record per stable GitHub repository identity, and Task-currentness is scoped through that Workspace-owned Repository.

A future Task origin may distinguish Owner-created, external-signal, or manual OpenOrc origins, but only GitHub/Owner-originated work is required for v1.

## 6.2 GitHub hierarchy is descriptive

GitHub parent/sub-issue relationships describe hierarchy, decomposition, and progress. OpenOrc mirrors enough authoritative hierarchy to display that structure and related progress, but hierarchy alone has no blocking or executability semantics in OpenOrc.

```text
parent issue
├── sub-issue
└── sub-issue

→ descriptive hierarchy
→ no inferred blocking
→ no inferred non-executability
```

An issue may create an OpenOrc Task whether or not it has a parent or sub-issues, provided the issue is otherwise eligible. OpenOrc must not derive an `is_leaf`, `is_composite`, or equivalent workflow guard from hierarchy. Parent closure and hierarchy maintenance remain GitHub/Owner concerns.

## 6.3 GitHub issue dependencies define blocking and sequencing

GitHub issue dependencies are authoritative for blocking and sequencing. OpenOrc consumes GitHub's explicit `blocked by` / `blocking` dependency semantics and mirrors only the relationship/state needed for deterministic orchestration and UI. Parent/sub-issue relationships never substitute for dependency state.

For Task intake and start eligibility, OpenOrc relies on GitHub's authoritative current blocking state rather than inventing its own blocker-resolution rules. A GitHub issue that GitHub currently reports as blocked cannot begin an OpenOrc Task; when GitHub no longer reports it as blocked, hierarchy alone does not prevent work from starting. If OpenOrc cannot authoritatively establish the blocking state needed for the decision, it fails closed.

There is no private OpenOrc implementation-phase graph beneath a GitHub issue. GitHub hierarchy may organize larger work, while explicit GitHub issue dependencies express sequencing where required. Each GitHub issue that enters OpenOrc has its own plan, authorization, implementation, PR, review, and merge decision.

## 6.4 PR cardinality

One executable Task has zero or one canonical `TaskPullRequest` in v1. OpenOrc does not introduce speculative replacement-PR history: if the canonical PR is closed unmerged, the Task blocks rather than automatically replacing it with another PR.

The canonical PR is one mutable GitHub reconciliation object whose observed head may change over time. Multiple immutable PR ReviewIterations may therefore reference the same `TaskPullRequest`; each review result is bound to the exact `head_sha` reviewed, and a later head invalidates prior acceptance without changing PR identity.

## 6.5 GitHub API vs runtime Git access

OpenOrc is the GitHub workflow API integration layer. It may read/write workflow-relevant data such as issues, hierarchy/dependencies, comments, labels where useful, PR metadata, PR creation, CI/check metadata, and durable milestone/status comments. Producer reasoning may author human-readable PR content, but workflow-level PR creation/mutation remains an OpenOrc operation.

The Producer runtime separately requires repository-level Git access to:

- clone/fetch;
- create worktrees/workspaces;
- edit code;
- commit;
- push non-protected feature branches.

For an OpenOrc-managed Cline Box, Git operations use the Owner's existing Profile-scoped GitHub App user authorization rather than a separate runtime/bot GitHub identity. OpenOrc v1 does not attempt to further narrow that human credential per Workspace or branch: its effective GitHub authority is the normal intersection of the Owner and App permissions, and GitHub remains authoritative for repository rules and protected-branch enforcement. The runtime receives no additional GitHub authority beyond that existing Owner authorization.

Conceptually:

```text
OpenOrc
├── GitHub workflow API permissions
└── runtime control endpoint access

Agent Runtime
├── git clone/fetch
├── git worktree/workspace
├── git commit
├── git push feature branches
└── no requirement for issue/comment/label workflow permissions
```

GitHub comments and labels may record workflow milestones but never become commands that grant Producer authority.

The review-cleared plan is published through OpenOrc as an Owner-attributed GitHub issue comment. OpenOrc does not rewrite the authoritative issue body merely to publish its execution plan; this keeps the issue requirements stable and prevents the plan publication from being mistaken for an authoritative requirements change. OpenOrc records that the Producer/Reviewer workflow produced and cleared the plan, while GitHub records the accountable human who published it.

## 6.6 GitHub identity and reconciliation keys

OpenOrc persists stable GitHub identifiers appropriate to the object being reconciled rather than relying on mutable presentation data. Repository, issue, pull-request, webhook, and related GitHub objects retain their stable numeric/global identifiers where available; repository-local issue/PR numbers remain useful addresses; exact commit SHAs identify code review subjects; webhook delivery GUIDs identify inbound deliveries for deduplication.

Mutable names, URLs, labels, branch names, and similar presentation/routing values may be persisted where useful, but they are not the sole identity for deterministic reconciliation. A branch name identifies the Task branch; its exact current commit SHA identifies the code state.

---

# 7. Workspace, Authentication, and Connections

## 7.1 Workspace isolation

Workspace is a first-class security, ownership, configuration, and engineering-context boundary from v1.

One authenticated user may own multiple independent Workspaces:

```text
Account
├── Workspace: Babelbeez
└── Workspace: OpenOrc
```

A Workspace scopes:

- Projects;
- Repositories;
- Tasks;
- Agent Runtime Connections;
- OpenOrc-owned Agent Runtime control-endpoint authentication/configuration;
- OpenOrc-managed runtime configuration and secure secret references where that Workspace uses a managed runtime;
- GitHub App installation/repository authorization mappings;
- runtime bindings/profiles;
- ReviewLoop limits and optional Workspace guidance;
- workflow/audit state.

Workspace-scoped OpenOrc authentication or configuration from one Workspace must not become implicitly visible in another. The deliberate exception is Profile-scoped GitHub user authorization: the same authenticated human may use their own authorization across their own Workspaces, but it is never shared with another Profile. In v1, that same authorization may also underlie Git access from more than one OpenOrc-managed Cline Box belonging to the Profile; its effective GitHub reach may therefore span repositories associated with several of that Profile's Workspaces. This cross-Workspace credential reach for one human is an accepted v1 tradeoff and does not create shared OpenOrc workflow/runtime state between those Workspaces.

OpenOrc supports two runtime-ownership modes without changing the workflow model. **Externally managed/BYO runtimes** are Owner-controlled execution environments whose filesystem, provider/MCP/tool/repository credentials, local configuration, upgrades, and operations remain governed by that runtime's own isolation/security model. **OpenOrc Cloud-managed runtimes** remain separate execution environments outside the shared control-plane process boundary. Cloud may initially provision and hydrate their compute, hibernate eligible Workspace runtimes by snapshotting the hydrated environment and destroying compute, restore them later, and retain Workspace-scoped configuration and secret material required for operation and clean rebuild. Managed-runtime infrastructure/Cline-native secret material remains isolated by Workspace and behind the secure secret boundary. The one explicit v1 exception is the Profile-scoped Owner GitHub authorization described above: Cloud may make that same human authorization usable for Git on multiple managed Boxes belonging to that Profile. Secret material is never surfaced in agent prompts, logs/telemetry, or ordinary application responses.

For the OpenOrc Cloud reference topology, each managed Workspace receives one persistent development environment/ClineBox that may contain several repositories and concurrent Task role sessions subject to configured capacity. Normal hibernation is blocked while nonterminal Tasks depend on live sessions. One qualified snapshot cycle preserved native authentication/configuration, repository/filesystem state and stopped records/transcripts without Owner reauthentication; this does not promise active-session portability or clean-rebuild survival. Host/SSH readiness is separate from managed Cline READY: Cloud starts Hub/dashboard as applicable, re-resolves current control credentials and verifies public remote attachment. Credentials rotated in two observations but every-start rotation is unproven. One completed stopped record re-projected failed and accumulated usage reset across the new Hub lifetime. Orderly poweroff succeeded without explicit Hub drain/stop; further quiesce policy is a Cloud design choice. Restored instances regenerate SSH host keys: Cloud adopts by guarded restore provenance and re-pins, never source-key equality. Clean rebuild may require Owner native reconfiguration. The pinned dashboard has a known headless Cline-account OAuth/browser-launch limitation without an OpenOrc workaround. These are Cloud deployment facts, not universal Core operations.

Runtime environments commonly contain repository clones/worktrees, filesystem access, Git/CLI authentication, MCP/tool credentials, local environment configuration, development dependencies/toolchains, runtime rules/history, and provider/model configuration. Cross-Workspace runtime sharing is never assumed safe: it is valid only when the runtime provides a demonstrably sufficient isolation domain for filesystem, credentials, configuration, tools, and working contexts. Otherwise each Workspace requires separate runtime isolation.

Multi-user membership, invitations, team roles, and enterprise RBAC may be added later without changing this boundary.

## 7.2 User and application authorization

v1 uses **GitHub as the only end-user sign-in provider**, with Supabase Auth providing the authentication/session layer and an OpenOrc-owned `Profile` as the canonical application identity. Email/password and other social sign-in providers are not part of the v1 product surface.

```text
GitHub user identity
↓ OAuth sign-in
Supabase Auth user
↓ 1:1
OpenOrc Profile
↓
Workspace ownership / OpenOrc authorization
↓
FastAPI verifies Supabase JWT and resolves Profile
↓
OpenOrc authorization rules
```

GitHub sign-in establishes who the human Owner is, but the Supabase/GitHub sign-in provider token is **not** reused as OpenOrc's repository credential. Repository access and GitHub workflow operations use the OpenOrc GitHub App through two distinct authorization modes described below: installation authorization for infrastructure/routing/reconciliation, and Profile-scoped user-to-server authorization for Owner-accountable GitHub mutations.

`auth.users` remains Supabase-controlled authentication infrastructure. OpenOrc application/domain relationships reference `Profile`, not Supabase's restricted Auth table directly. In v1 the Profile uses the corresponding Supabase Auth user UUID as its 1:1 identity, while profile/application fields remain fully OpenOrc-owned. Supabase establishes authentication identity; OpenOrc decides what the resolved Profile may authorize or change. OpenOrc may retain the stable GitHub user identifier required to prove that a Profile-scoped GitHub App user authorization belongs to the same human identity that signed in; mutable GitHub login names, display names, and email addresses are presentation metadata rather than identity authority.

OpenOrc is greenfield on Supabase's current key/signing model: browser clients use a publishable API key, server-side OpenOrc components use a secret API key for privileged Supabase access, and Supabase Auth JWTs use the project's current signing-key/JWKS model. Privileged server credentials remain backend/worker-only and are never exposed to the browser.

Backend authorization checks remain explicit even when the reference deployment effectively has a single owner/admin user.

Protected actions include:

- implementation authorization;
- Reviewer discussion as the authenticated Owner;
- Workspace guidance/configuration changes;
- scoped responses to Producer/runtime `RuntimeRequest`s;
- retry/cancel controls;
- runtime/configuration changes;
- PR authorization/creation;
- merge decisions.

## 7.3 Agent Runtime Connections and GitHub installations

In v1, `Connection` is the OpenOrc domain abstraction for an **Agent Runtime route**, not a universal external-service credential container. This matches the implemented Phase 1 domain: a Workspace-scoped Connection describes how OpenOrc reaches a runtime such as Cline Hub, its safe non-secret configuration, Owner-controlled eligibility, admission capacity, and any OpenOrc-owned authentication reference required for the runtime control endpoint.

```text
Workflow role
↓
WorkflowRoleBinding
↓
Agent Runtime Connection
↓
Agent Runtime adapter
↓
External runtime
```

A `WorkflowRoleBinding` references an Agent Runtime `Connection` plus per-role configuration. Producer and Reviewer may share a Connection while retaining separate Task sessions and working contexts. The binding stores opaque configured provider/model identifiers and an optional Owner-authored role-prompt Markdown override. A NULL override uses the current shipped OpenOrc default for that role; defaults are not copied into Workspace configuration. Runtime mode is workflow-derived, not an Owner setting. No generic runtime-configuration bag or catalog is required.

GitHub is intentionally **not** represented as an Agent Runtime Connection. Repository routing uses a separate Workspace-scoped GitHub App installation concept, represented durably by a `GitHubInstallation` or equivalent repository-integration record. That record stores stable installation/account identity and observed/configuration state needed for reconciliation, but no human user token or PAT. Repositories are routed through an installation that currently grants the OpenOrc GitHub App access to them. The exact foreign-key shape between Workspace, installation, and Repository is an implementation decision, but selection must be explicit and deterministic; OpenOrc must never guess among multiple installations at execution time.

Human GitHub authorization is a distinct Profile-scoped concept rather than a field on `GitHubInstallation`. One physical GitHub App installation may therefore be represented independently in several Workspaces and used by several OpenOrc users, while each Profile retains its own user-to-server authorization and no Workspace shares another human's credential or OpenOrc state.

The workflow/domain layer depends on semantic OpenOrc operations. Adapters own provider-specific API, webhook, authentication-token minting, and transport mechanics. This deliberate split avoids forcing unrelated concepts such as runtime session capacity or provider/model observations onto GitHub integration records.

## 7.4 Universal v1 agent-session contract

The universal v1 contract is intentionally small:

```text
create_session(task, role)
    → session_id
    OR normalized error

send(session_id, message)
    → response
    OR normalized error
```

`session_id` is opaque to OpenOrc and exists only so later interactions can be routed to the exact external Task/role context.

There is no universal requirement for:

```text
interaction_id
session_status polling
inspect_session
close_session
structured-output provider APIs
webhooks
streaming
tool calling
```

An adapter may use any of these internally when its provider supports them.

Runtime-specific execution controls, scoped RuntimeRequest responses, Owner-triggered runtime activity cancel/resume, inspection, snapshots, and event streaming remain runtime-adapter concerns rather than universal session operations. Where an adapter exposes cancel/resume, those controls operate on the exact already-bound external Task session and do not by themselves cancel the OpenOrc Task, end/replace the TaskAgentSession, or create a new conversational context.

Runtime telemetry is an optional adapter capability. A runtime that exposes authoritative session/activity events or snapshots may make them available through its adapter; an integration that does not expose such telemetry remains valid and reports runtime activity as unknown rather than unhealthy.

Native external UI/deep-link exposure is likewise an optional adapter capability. Where an external runtime/provider offers a useful management or session-inspection interface, the adapter may expose a supported external link for the OpenOrc UI without making that interface part of the universal session contract.

## 7.5 v1 Agent Runtime adapter and role configuration

v1 deliberately implements Cline Hub as its first Agent Runtime because Cline already provides the execution platform OpenOrc intends to govern: persistent agent sessions, model/provider flexibility, tools, filesystem/shell access, and runtime-owned credentials. Additional Agent Runtimes may be added when user demand justifies them without changing OpenOrc workflow semantics.

```text
Producer
→ ClineAdapter
→ Cline Hub

Reviewer
→ ClineAdapter
→ Cline Hub
```

Producer and Reviewer are independently bound workflow roles even when both bindings point to the same physical Cline Hub. Each role creates its own isolated Task sessions and may use different runtime session configuration.

The supported Cline baseline is CLI `3.0.68`, Hub/runtime `@cline/core 0.0.90`, and public client `@cline/sdk 0.0.90`. OpenOrc uses `ClineCore` remote mode with opaque per-role `providerId`/`modelId` strings. Native Cline UI supplies the model ID; provider ID is a separate identifier and is not assumed discoverable there. OpenOrc does not maintain a model catalog, configured-provider registry, or required effective provider/model readback. Cline owns provider authentication and native account/configuration. Selection usability is validated lazily on the first turn; a failure before valid `session_ready` is a construction/configuration failure, not ordinary workflow completion.

This ownership boundary applies equally to externally managed and OpenOrc Cloud-managed Cline Boxes. Making the host managed does not make OpenOrc canonical owner of Cline-native provider credentials or configuration. OpenOrc must not parse private Cline files or duplicate native dashboard state to infer configuration readiness.

OpenOrc does not build a second permission system on top of Cline. In v1 it uses Cline's native Plan/Act mode boundary as the role capability boundary:

```text
Reviewer
→ PLAN mode for the Task lifetime

Producer
→ PLAN mode during planning
→ ACT mode only after implementation authorization
→ ACT mode for implementation and PR remediation
→ does not return to PLAN within the same Task after planning is cleared
```

On the qualified baseline, Cline PLAN permits read-only commands and guards mutating commands; ACT is execution-capable. OpenOrc uses this native mode/tool boundary without a second permission system. Mode is fixed at construction: per-turn `send(mode)` tags input only. Producer PLAN→ACT uses the same-ID rebuild: reconcile settled work, `readMessages(sessionId)`, await `stop(sessionId)`, then `start` with the existing ID, raw transcript as `initialMessages`, target mode and full construction bundle. It preserves the Task session and one-time initialization; no continuation/mode-notice prompt is required. Original record `metadata.mode` is not current-mode truth. Native tool approvals do not cross the qualified OpenOrc seam: register no interactive executors or approval callbacks, and suppress `ask_question`. Reviewer stays PLAN; GitHub owns CI/check execution.

Selected non-secret provider/model identity is captured when the Task role session is established. Later Workspace configuration changes affect future sessions and do not rewrite initialized session configuration. Requested provider/model values are not verified effective identity; runtime-reported provenance is optional and may be NULL. Role prompts cannot change during dependent nonterminal Tasks; §9.4 defines that guard. This keeps rebuild inputs stable without storing historical prompt snapshots.

The architecture must permit additional Agent Runtime adapters later without requiring a generic capability marketplace or universal adapter SDK in v1. A future runtime or bridge backed by a nominally stateless completion/response provider is usable only if it can deterministically establish isolated Task context, preserve continuity, and expose an opaque stable routing identifier to OpenOrc.

### v1 runtime topology and concurrency

v1 configures exactly one runtime binding for each workflow role. Each binding references one Agent Runtime Connection plus role-specific session configuration. Producer and Reviewer bindings may reference the same Cline Hub Connection or separate Connections. Runtime count and Task-session capacity are separate concerns.

```text
Producer role
→ one configured runtime binding
→ zero or more isolated Producer Task sessions, according to runtime capacity

Reviewer role
→ one configured runtime binding
→ zero or more isolated Reviewer Task sessions, according to runtime capacity
```

Each Agent Runtime Connection has an Owner-configured OpenOrc session-capacity value. The default is `1`, so concurrency is a conscious Owner choice rather than an implicit deployment assumption. This is an OpenOrc admission/backpressure setting for the Owner's deployment infrastructure, not a capacity value discovered from or enforced by Cline. OpenOrc may progress multiple executable Tasks concurrently when the underlying runtime Connection(s) have enough configured aggregate capacity for all role sessions the Task requires. If Producer and Reviewer share one Cline Hub Connection, starting one Task consumes two slots from that Connection's OpenOrc capacity pool; if they use separate Connections, each role consumes one slot from its own Connection. Effective Task concurrency is therefore bounded by Owner-configured Connection capacity rather than by a global single-Task OpenOrc limit.

The qualified managed topology is one persistent CLI-owned Hub/Box that hosts distinct Producer/Reviewer sessions with separate ordinary repository checkouts. Per-session `cwd` and `workspaceRoot` both point to that role checkout. Root repository `AGENTS.md` is naturally consumed without OpenOrc rule-file hydration. Directory separation is working-context isolation, not a Workspace security boundary. Separate runtime deployments remain a valid hardened choice.

OpenOrc must not encode a global `current_task`, singleton role session, or other assumption that prevents parallel Task execution.

Multiple configured runtimes per role and a generic runtime-pool layer remain post-v1. Pool scheduling, failover, and dynamic membership are not v1 responsibilities. This does not prohibit the concrete OpenOrc Cloud managed-Cline lifecycle: Cloud may provision, hibernate, restore, or cleanly rebuild the one configured Workspace runtime behind a role/Connection as a hosted capability without introducing a universal Core runtime-pool/provisioning framework.

## 7.6 Authentication ownership

Authentication ownership follows direct consumption and runtime ownership:

```text
Human signs in to OpenOrc
→ GitHub identity via Supabase Auth

OpenOrc reads/reconciles GitHub repository state
→ GitHub App installation authorization

OpenOrc performs an Owner-accountable GitHub mutation
→ Profile-scoped GitHub App user-to-server authorization

OpenOrc calls an Agent Runtime control endpoint
→ OpenOrc owns any required control-endpoint authentication

Externally managed runtime calls model provider / MCP server / tool
→ runtime owner supplies and owns required authentication/configuration

OpenOrc Cloud-managed Cline Box calls model provider / MCP server / tool
→ Cline-native authentication/configuration remains owned by the Cline Box and configured through supported Cline surfaces
→ OpenOrc Cloud owns only infrastructure/control credentials it directly consumes
→ a retained hibernation snapshot may preserve the hydrated runtime environment for normal resume, but is not a substitute for supported Cline configuration ownership

OpenOrc Cloud-managed Cline Box performs Git clone/fetch/push
→ same Profile-scoped GitHub App user-to-server authorization used for the Owner
→ no separate Cline/bot identity or per-Workspace OAuth grant in v1
→ Cloud owns eventual short-lived token materialization/refresh/revocation/rebuild mechanics
```

### GitHub user identity

GitHub is the sole v1 sign-in provider. Supabase Auth handles the OAuth sign-in/session boundary and issues the JWT OpenOrc verifies. The resulting Supabase user UUID remains the 1:1 identity of the OpenOrc `Profile`. Sign-in proves the human identity but does not itself authorize the OpenOrc backend to operate on arbitrary repositories.

OpenOrc does not use the human's Supabase/GitHub sign-in provider token as its durable GitHub workflow credential. This keeps authentication-to-OpenOrc separate from GitHub App authorization and avoids coupling repository automation to a browser sign-in token lifecycle.

### GitHub repository authorization

The v1 GitHub workflow integration uses an OpenOrc GitHub App with two deliberately separate authorization modes.

**Installation authorization** answers which repositories the App may access. An Owner with sufficient GitHub authority installs the app on a personal account or organization and grants it access to selected repositories. OpenOrc persists the stable installation identity and enough observed installation/repository state to route and reconcile operations. Short-lived installation access tokens are minted from deployment-held App credentials and are used for infrastructure operations such as installation/repository capability validation, authoritative reads, webhook-driven reconciliation, and missed-delivery/background recovery. Installation access tokens are ephemeral and are not persisted as ordinary application data.

**User-to-server authorization** answers which accountable human OpenOrc is acting for when it changes the GitHub engineering record. Each OpenOrc Profile that uses Owner-accountable GitHub mutations authorizes the same OpenOrc GitHub App on their own behalf. OpenOrc must prove that authorization belongs to the same stable GitHub human identity as the Profile's GitHub sign-in; mutable login/email are not sufficient. The resulting user access token is effective only where both that human and the installed App have the required repository permission. The durable per-human refresh credential is Profile-scoped secret material stored through the supported encrypted secret boundary. Short-lived user access tokens are minted/refreshed as needed; Core uses them for Owner-attributed GitHub operations, and OpenOrc Cloud may also make that same authorization usable for HTTP Git on an OpenOrc-managed Cline Box. v1 does not create a separate OAuth grant per Workspace or a separate Cline/OpenOrc bot identity for those Git operations.

One physical GitHub App installation may serve several humans working on the same repository. Each may connect that repository independently through their own OpenOrc account/Workspace, with a Workspace-scoped `GitHubInstallation` representation of the shared external installation and a separate Profile-scoped user authorization. One Profile-scoped authorization may be reused by that same human across their own Workspaces and, in v1, may have effective access to repositories associated with several of them. Workspace isolation, runtime configuration, Tasks, and agent provenance remain independent despite that accepted shared human-credential reach.

Owner-accountable GitHub mutations include the review-cleared plan publication, canonical pull-request creation, exact-head merge requests, and later mutations whose semantics are the human engineer changing the durable engineering record. These operations must use the Profile-bound user-to-server authorization. On an OpenOrc-managed Cline Box, repository Git operations such as clone/fetch/push use that same Owner authorization lineage even though the Git process runs inside the managed runtime; they do not create a separate GitHub engineering identity. Missing, revoked, expired, mismatched, or insufficient human authorization fails closed and must never fall back to an installation-authenticated write or PAT. OpenOrc and its agent roles do not receive their own GitHub engineering identities.

Removing repository access or uninstalling the App makes that loss of authority visible through reconciliation and blocks affected workflow. Revoking a Profile's user authorization blocks that Profile's Owner-accountable writes without changing the installation route or another human's independent authorization.

The GitHub App must be configured with the repository permissions and webhook subscriptions required by the v1 workflow, including the effective capabilities needed to:

- read issue state, sub-issues/relationships, and dependencies;
- publish the review-cleared plan as an issue comment on behalf of the Owner;
- read repository/branch/commit state;
- create pull requests on behalf of the Owner and reconcile them authoritatively;
- read CI/check and commit-status state;
- request merge on behalf of the Owner with the exact expected head SHA;
- receive the relevant GitHub App webhook events.

Exact GitHub App permission names are an adapter/deployment detail that must be validated against the concrete GitHub API operations used by the implementation. The application validates both the effective installation/repository capability and, for Owner-accountable mutations, the effective user/App permission intersection rather than merely assuming that an installation ID or authenticated Profile is sufficient.

The GitHub App's private key, user-flow client secret, and webhook secret are deployment/bootstrap secret material. They are not Workspace-owned ordinary application data and remain available only to the backend components that need them. OpenOrc Cloud operates its own GitHub App. A self-hosted deployment is responsible for configuring an appropriate GitHub OAuth application for Supabase sign-in and an appropriate GitHub App for repository authorization. Per-human GitHub App refresh credentials are different: they are row-varying Profile-scoped OpenOrc secrets and are stored encrypted through the supported secret boundary rather than deployment configuration. Self-hosting convenience does not justify weakening this boundary or reintroducing PAT-based workflow authentication.

### Runtime and other authentication

For an **externally managed/BYO runtime**, the runtime owner normally owns model-provider/inference-router credentials, MCP OAuth/API credentials, runtime-local development-tool credentials, runtime repository/Git credentials, runtime account/session credentials, and other credentials used only inside that execution boundary. OpenOrc may select supported session configuration without becoming canonical owner of those external-runtime secrets or catalogs.

For an **OpenOrc Cloud-managed Cline Box**, host lifecycle ownership does not create a provider/MCP/tool secret-custody exception. Cline-native authentication and runtime configuration remain Cline-owned and may be restored with the hydrated environment when available or re-established by the Owner through supported Cline surfaces after a clean rebuild. Managed-Box Git is different: it uses the same Profile-scoped GitHub App user authorization that Core owns for the Owner, so Git operations remain Owner-attributed rather than introducing a second Cline-specific GitHub identity. The same authorization may have effective access to repositories associated with several of that Profile's Workspaces; v1 accepts that reach and does not require a per-Workspace OAuth grant. Core owns the authorization model and remains unaware of Cline Box provisioning. Cloud depends on Core and owns the eventual mechanics that make the authorization usable on a managed Box, including token materialization/refresh/revocation/rebuild handling. Those production Cloud mechanics are deferred until deployed Core exposes the real authorization capability and do not block the ClineAdapter contract.

Raw authentication material OpenOrc must hold must live behind a secure storage or deployment-secret boundary, not as raw values in ordinary application tables or ordinary API responses. The Profile-scoped GitHub App refresh credential remains Core-owned secret state; any future short-lived user-token delivery to a managed Cline Box is a Cloud integration concern rather than a new Core credential/domain abstraction. Pass 1 does not introduce generic `Credential`, `CredentialBinding`, or `SecretStore` entities. Bootstrap/deployment secrets remain appropriate in GitHub Secrets / GitHub Environments or equivalent deployment configuration.

Each deployed component receives only the authentication material it directly requires. Runtime secrets must never be surfaced into agent prompts, logs/telemetry, or ordinary OpenOrc API responses. Where Producer and Reviewer share one credential domain, role separation relies on isolated sessions plus the validated runtime configuration/isolation model rather than pretending the underlying secrets are distinct.

A shared Agent Runtime may serve more than one OpenOrc Workspace only if its own credential, working-context, and configuration isolation is sufficient to preserve the Workspace security boundary. Otherwise those Workspaces require separate runtime instances or isolated runtime configuration domains. This is a runtime deployment constraint, not permission to weaken Workspace isolation.

OpenOrc manages the lifecycle of authentication it owns directly. Externally managed runtime authentication remains that runtime owner's responsibility. Managed-runtime authentication/configuration may be managed by OpenOrc Cloud only to the extent required to reproduce and operate that Workspace's managed runtime through validated supported surfaces.

---

# 8. Core Domain Model

Persistence follows product semantics rather than allowing database tables to define the product accidentally.

The v1 core concepts are:

```text
Profile
Workspace
Project
Repository
GitHubInstallation
Task
TaskAgentSession
PlanRevision
ReviewLoop
ReviewIteration
OwnerGate
Execution
RuntimeRequest
TaskBlock
TaskPullRequest
WorkflowEvent
Connection
WorkflowRoleBinding
```

Ownership hierarchy:

```text
Supabase Auth User
↓ 1:1
Profile
↓
Workspace
↓
Project
↓
Repository
↓
Task
```

A Workspace may own multiple Projects, GitHub App installation records, Agent Runtime Connections, runtime bindings/configuration, optional guidance, and policies.

A Project may own one or more Repositories. A Repository belongs to exactly one Project inside a Workspace and is routed through an authorized GitHub App installation with access to that repository. A Task is one durable unit of executable engineering work backed by one authoritative GitHub issue in v1.

Because Workspace is a security boundary, meaningful Workspace-owned operational rows carry `workspace_id` directly even when derivable through joins. That direct scope supports explicit authorization, indexing, isolation, and parallel work across Workspaces; persistence constraints must ensure the directly stored Workspace identity agrees with the owning parent relationship rather than becoming a competing source of truth.

Project ownership for a Task is derived through its Repository rather than duplicated as a competing field.

## 8.1 GitHubInstallation

`GitHubInstallation` is the durable Workspace-scoped routing identity for an installation of the OpenOrc GitHub App. It is not an Agent Runtime `Connection` and does not store a PAT or any human user-access/refresh credential.

Conceptually:

```text
GitHubInstallation
├── workspace_id
├── github_installation_id
├── github_account_id
├── account_login / observed display metadata
├── installation/repository-access state
└── created_at / updated_at
```

The stable GitHub installation/account identifiers are authority/routing facts; mutable display names are observed metadata. Repository mappings must resolve deterministically to an installation that currently grants the GitHub App access to that repository. A Workspace may have more than one installation, for example when it spans repositories owned by different GitHub organizations/accounts. The same external `github_installation_id` may be represented independently in multiple Workspaces owned by different OpenOrc users; the external installation is shared GitHub infrastructure, not shared OpenOrc authority or state. Loss of installation or repository access is reconciled as an authorization/integration condition and must not silently fall back to a human user authorization or PAT.

Exact persistence shape is introduced with the Phase 2 GitHub integration because Phase 1 deliberately implemented Agent Runtime `Connection` persistence only.

## 8.2 GitHubUserAuthorization

`GitHubUserAuthorization` is the durable Profile-scoped binding proving which GitHub human OpenOrc may act for when performing Owner-accountable GitHub mutations. It is separate from `GitHubInstallation`: installation routing answers **where the App may operate**; user authorization answers **which human OpenOrc is acting for**.

Conceptually:

```text
GitHubUserAuthorization
├── profile_id
├── github_user_id
├── observed login / presentation metadata
├── opaque refresh-secret reference
├── authorization/currentness metadata
└── created_at / updated_at
```

The stable `github_user_id` is the identity-binding fact. The authorization must be proven to belong to the same GitHub identity that backs the Profile's GitHub sign-in; mutable login/email are never sufficient identity authority. Raw user access/refresh tokens never appear in this domain record or ordinary application tables. The durable refresh credential is encrypted in the supported secret store and referenced opaquely. Short-lived access tokens remain ephemeral; Core mints/refreshes them through the trusted GitHub authentication boundary, while Cloud may later deliver/use such short-lived authorization for managed-Box HTTP Git without changing this Profile-scoped domain model.

Revocation, expiry that cannot be refreshed, identity mismatch, or insufficient repository permission makes Owner-attributed mutation unavailable for that Profile. OpenOrc must not substitute installation authentication, another Profile's authorization, or a PAT. Account deletion must remove the OpenOrc-owned Profile refresh secret without uninstalling the GitHub App or deleting/mutating GitHub engineering artifacts.

A single external GitHub App installation may therefore be paired with many independent `GitHubUserAuthorization` records belonging to different OpenOrc Profiles, including humans collaborating on the same repository through separate OpenOrc accounts/Workspaces.

## 8.3 TaskAgentSession

`TaskAgentSession` is the durable OpenOrc routing binding between one Task/role and one externally owned conversational session. It does not contain or reconstruct conversational context.

Conceptually:

```text
TaskAgentSession
├── workspace_id
├── task_id
├── role
│   ├── PRODUCER
│   └── REVIEWER
├── connection_id
├── external_session_id
├── lifecycle_status
├── effective_runtime_config_ref / snapshot
├── initialized_at
└── ended_at
```

For each executable Task there is exactly one OpenOrc `TaskAgentSession` binding per configured role once that role binding is established:

```text
(Task, PRODUCER) → one TaskAgentSession → one external session identity
(Task, REVIEWER) → one TaskAgentSession → one external session identity
```

Session creation is idempotent at the Task/role boundary. A repeated request to establish `(Task, role)` must return/reuse the same binding or fail deterministically. Once a binding has successfully initialized an external session, its `external_session_id` must never be overwritten with a replacement session.

A Cline Hub/process outage does not by itself mean the Task session was lost: if the same persisted external session/context can be restored, the same `TaskAgentSession` remains valid. If the exact external session/context is genuinely lost or cannot be restored, the binding becomes `LOST` and the Task follows the `AGENT_SESSION_LOST` blocking/recovery path. OpenOrc does not manufacture a new session and pretend Task continuity survived.

A binding becomes usable only after the adapter has established a fresh external context, delivered the OpenOrc initialization protocol, received the required `session_ready` v1 response, and validated it.

Initialization failures, timeouts/stalls, and surfaced provider/protocol failures remain recovery inputs; they never authorize replacement of a previously confirmed Task session.

## 8.4 Current state, mutability, and concurrency

`Task.status` is the canonical current primary workflow state. The Task may hold explicit current-object pointers such as `current_plan_revision_id` and `current_owner_gate_id`; those pointers answer which immutable related record is current and must not duplicate that record's semantic contents or result fields. There is no singular `current_execution_id`: a Task owns Execution history and the model must remain compatible with future multiple active Executions.

OpenOrc follows a one-fact/one-canonical-home rule. Historical evidence becomes immutable once finalized: PlanRevisions, completed ReviewIterations, resolved OwnerGates, completed Executions, resolved RuntimeRequests, resolved TaskBlocks, and WorkflowEvents are not rewritten to manufacture a different history. Mutable operational/configuration records such as Task current state, active lifecycle metadata, `TaskPullRequest` reconciliation state, Connections, role bindings, and Workspace guidance change only where their semantics require it.

Each Task carries an opaque `state_token` UUID used for optimistic stale-operation protection. Every authoritative Task-state mutation replaces the token. Workflow-changing commands/jobs bind themselves both to the observed `state_token` and to the exact relevant subject/authority context, such as the current PlanRevision, OwnerGate, PR identity/head SHA, or equivalent subject. A mismatch means the operation is stale and its effect must not be applied.

Workflow transitions use short Postgres transactions and may use row locks where useful to verify and atomically update current state. No database transaction remains open while OpenOrc waits on GitHub, Cline, model inference, or another external system. Externally consequential/asynchronous logical operations also carry durable unique operation identity so replay of the same command can be recognized independently of stale-state detection.

## 8.5 Workspace guidance

A Workspace may store one optional Owner-authored guidance text used to influence agent emphasis or engineering conventions. The default is blank.

Workspace guidance is data, not a replacement prompt template. OpenOrc decides where guidance is inserted into its own initialization or workflow interactions, and omission/blank guidance is always valid.

Guidance cannot modify OpenOrc initialization prompts, formal JSON schemas, workflow-control semantics, exact review-subject identity, session semantics, authority rules, communication topology, or state transitions.

OpenOrc stores only the current Workspace guidance. It does not persist guidance versions, hashes, historical snapshots, or per-interaction prompt-use metadata. Workflow/audit history records resulting workflow facts, exact subjects, structured results, actors, and authority decisions instead.

## 8.6 Deliberately absent v1 domain concepts

The v1 domain does not include:

```text
AcceptedPlan
LiveExecutionPlan
ExecutionStep
ActiveTask
ImplementationState
DiscussionThread as conversational-memory storage
```

The review-cleared plan is a PlanRevision. Large work decomposition belongs in GitHub. Execution is a runtime attempt rather than a hidden phase. Conversational memory belongs to the connected role session.

Conceptually:

```text
Task
├── TaskAgentSession *
├── PlanRevision *
├── ReviewLoop *
│   └── ReviewIteration *
├── OwnerGate *
├── Execution *
│   └── RuntimeRequest *
├── TaskBlock *
├── TaskPullRequest *
└── WorkflowEvent *
```

Exact SQL representation remains an implementation decision.

---

# 9. Agent Sessions, Roles, Interaction Controls, and Protocol

Producer and Reviewer are logical workflow roles, not vendor identities.

v1 bindings are:

```text
Producer → ClineAdapter → Cline Hub → Producer Task session
Reviewer → ClineAdapter → Cline Hub → Reviewer Task session
```

The two roles are independently bound and initialized whether they share a Hub or use separate Hubs. OpenOrc supplies per-role opaque provider/model identifiers and runtime-neutral role prompts; Cline owns native authentication/configuration. The qualified construction bundle uses role Markdown in `config.rules`, blank `systemPrompt` so Cline builds its native harness, the role checkout as both `cwd` and `workspaceRoot`, workflow-derived mode, tools enabled, spawn-agent/agent-teams disabled, `interactive: true`, and `toolPolicies.ask_question.enabled = false`, without interactive client capabilities.

## 9.1 Task-scoped session lifecycle

When an executable Task begins, OpenOrc establishes one persistent session for each configured role before sending workflow-specific instructions.

```text
Task #123 begins
├── create_session(Task #123, PRODUCER) → Producer session P123
└── create_session(Task #123, REVIEWER) → Reviewer session R123
```

`create_session` is a semantic OpenOrc operation. The adapter must establish a fresh isolated external conversational context and expose an opaque `session_id`.

Session creation includes a mandatory readiness handshake. A runtime successfully allocating a session ID is not sufficient by itself; OpenOrc also verifies that the role-specific initialization message was processed and that the agent can speak the required protocol:

```text
OpenOrc requests create_session(Task, role)
↓
adapter establishes fresh external context
↓
adapter delivers the OpenOrc-owned role initialization prompt
↓
agent returns valid `session_ready` v1 JSON
↓
adapter validates response and returns opaque session_id
↓
OpenOrc marks TaskAgentSession usable
```

If initialization times out, fails, or produces an invalid readiness response, session creation fails. An allocated but unconfirmed context is not a healthy Task session.

For the rest of the Task, OpenOrc routes interactions through:

```text
send(P123, message)
send(R123, message)
```

Workflow state changes, ReviewLoop iterations, Executions, retries, RuntimeRequests, owner gates, and PR remediation do not create new sessions.

For Cline, runtime mode follows the authorized workflow without becoming a domain state. Reviewer stays PLAN. Producer begins PLAN, moves to ACT only on an already-authorized implementation control and remains ACT for remediation. The qualified public same-ID runtime rebuild preserves the exact external session identity and transcript; it does not replace the bound OpenOrc session. Re-supply the complete construction bundle on every rebuild, preserve raw public transcript messages verbatim and never resend initialization.

A Task reaching `COMPLETED` or `CANCELLED` ends OpenOrc's active use of the role-session bindings. v1 does not require a universal external `close_session` operation.

Unexpected loss of the exact bound context must not be silently repaired by creating a new one. While a runtime instance still exists, a supported reconnect/recovery path may restore access only to that same exact bound context. Genuine loss remains a deterministic blocking/recovery condition.

For an OpenOrc Cloud-managed runtime, normal hibernation or destruction is blocked while any nonterminal Task depends on live sessions on that runtime. Hibernation is a Workspace-environment lifecycle operation, not Task-session recovery: it is permitted only after those Task dependencies are gone. An explicit emergency-destruction escape hatch may deliberately destroy the runtime despite active dependencies; that action makes the affected exact sessions unrecoverable, causes the affected Task attempts to fail closed with session-loss context, and does not manufacture replacement sessions. Continuation uses the existing cancellation/archive + fresh-Task path, which creates fresh Producer and Reviewer sessions. OpenOrc does not preserve, copy, or reconstruct active Cline conversational/session history to bypass this guard.

## 9.2 OpenOrc initialization layer

The initialization prompt is deliberately thin and OpenOrc-specific. It must not duplicate, replace, or contradict the runtime/provider's own system prompt, agent harness, tool instructions, coding guidance, review methodology, repository-local instructions, or other native behavior.

v1 initialization is intentionally narrower than a role handbook. Its purpose is to establish the role-specific formal response vocabulary for the persistent session and complete the mandatory `session_ready` handshake. OpenOrc already owns the authoritative Task/role/session binding and does not require the model to echo that identity back in readiness output.

Layering:

```text
provider/runtime system harness
→ role Markdown composes additively through the adapter; Cline uses config.rules
→ repository-owned root AGENTS.md is naturally consumed where present
→ general agent behavior, tools, runtime semantics
→ may independently include repository/runtime-local instruction layers

OpenOrc role prompt
→ shipped role-behavior default or optional Owner override, separate from Workspace guidance
→ additive runtime configuration, subordinate to protocol/authority

OpenOrc initialization protocol
→ canonical Producer or Reviewer Markdown asset
→ renders only the canonical formal-response schemas relevant to that role
→ requests canonical `session_ready`

per-interaction OpenOrc workflow control
→ supplies the current semantic instruction, exact subject, and context
→ may include optional Workspace guidance where useful
→ adapter realizes the control through prose, structured artifact routing, a runtime-native action, or a demonstrated combination
```

The two canonical v1 initialization assets are:

- `src/openorc/protocol/initialization/producer.md`;
- `src/openorc/protocol/initialization/reviewer.md`.

Producer initialization renders the canonical `plan_result`, `implementation_result`, `pr_result`, and `session_ready` schemas. Reviewer initialization renders the canonical `review_result` and `session_ready` schemas and establishes that formal JSON review inputs are answered with `review_result`.

The Markdown contains controlled schema insertion points; schema bodies come from the canonical JSON files under `src/openorc/protocol/schemas/` and are not duplicated by hand. The assets are OpenOrc-owned and non-overridable.

The canonical v1 initialization deliberately does **not** add broader role, authority, Git, review-methodology, repository-discovery, or runtime-operation prose merely because such guidance might be useful in some environments. It must work in a vanilla supported Agent Runtime against an arbitrary repository without assuming `AGENTS.md`, `.cline/rules`, `CLAUDE.md`, `activeTask.md`, or any equivalent instruction layer exists. If such runtime/repository instructions do exist, they operate independently of the OpenOrc initialization contract.

For Cline, remote `start({prompt})` allocates without executing that prompt. Canonical initialization is the first `send()` and must return valid `session_ready` before READY. It never replaces the native harness or substitutes for the separate role prompt.

## 9.3 Owner ↔ Reviewer discussion

At Reviewer-related owner decision points, the UI provides `Discuss`, routing free-form conversation to the Task's existing Reviewer session.

OpenOrc does not create a separate discussion session and does not reconstruct Reviewer context from stored messages.

Discussion is advisory:

```text
Owner ↔ Reviewer free-form discussion
↓
no workflow transition
```

Even if discussion says a concern is resolved or a plan is acceptable, OpenOrc does not infer state change from prose. When formal acceptance is required, OpenOrc explicitly requests a structured `review_result` against the exact current review subject.

OpenOrc may persist discussion messages or metadata for UI/audit purposes, but such persistence is not the Reviewer's conversational-memory source of truth.

## 9.4 Formal schemas, role prompts, initialization, workflow controls, and Workspace guidance

OpenOrc distinguishes the following owned interaction categories:

```text
formal JSON schemas
→ canonical `.json` files shipped with OpenOrc
→ OpenOrc-controlled and non-overridable
→ carry explicit `schema_version` because structural compatibility matters

role prompts
→ runtime-neutral Producer/Reviewer Markdown defaults shipped with OpenOrc
→ optional freely configurable Owner override per role binding; NULL uses current default
→ subordinate to protocol, authority and workflow contracts

role initialization prompts
→ canonical Producer/Reviewer `.md` files shipped with OpenOrc
→ OpenOrc-controlled and non-overridable
→ establish the role-specific formal response vocabulary and readiness handshake
→ render canonical schemas from the JSON files into controlled insertion points

workflow controls
→ OpenOrc-owned semantic instructions/interactions for the current workflow action
→ non-overridable
→ prose exists only where demonstrated and useful
→ adapter may realize a control through prose, structured artifact routing, a runtime-native action, or a demonstrated combination

Workspace guidance
→ optional Owner-authored prose
→ blank by default
→ one current Workspace setting
→ injected only into OpenOrc-controlled locations where useful
→ never defines protocol, authority, session semantics, exact subject identity, or workflow transitions
```

Formal schemas are the sole machine-contract source of truth. Initialization Markdown must not maintain handwritten copies of schema definitions.

Role-prompt edits, including override reset or routing changes that would change the resolved prompt, are rejected while already-started nonterminal Tasks depend on that Workspace role. Same-value writes are no-ops. Admission and edits serialize through the role-binding boundary, covering CONNECTING and recovery gaps as well as active turns. Shipped default prompt changes must not be activated for affected Tasks until those Tasks finish or cancel. OpenOrc stores only the current optional override, with no prompt copies, versions, hashes or history. Provider/model edits may affect future sessions while existing selected IDs remain captured in the initialized non-secret session configuration.

The canonical v1 initialization/control prose inventory is deliberately small; separate role-behavior defaults are not protocol assets:

```text
initialization/producer.md
initialization/reviewer.md
controls/plan.md
controls/pr_compose.md
```

There is no canonical `review.md`, `remediate.md`, or `implement.md` in v1, and implementation code must not synthesize competing hidden prompt templates for them.

The semantic workflow-control vocabulary is broader than the prose-file inventory. v1 uses PLAN, REVIEW, REMEDIATE/REVISE, IMPLEMENT, and PR_COMPOSE semantics:

- PLAN is realized with the canonical terse planning prose control.
- REVIEW is an exact-subject interaction. Planning review is bound to the exact supplied plan subject; PR review is bound to the exact canonical PR identity and reconciled head SHA. The Reviewer returns `review_result`.
- REMEDIATE/REVISE uses the exact valid `CHANGES_REQUESTED review_result` as the substantive Producer input. Planning remediation produces another `plan_result`; PR remediation produces another `implementation_result`. No separate remediation prose asset is required.
- IMPLEMENT is emitted only after implementation authority has been established above the interaction layer. For Cline v1, the concrete realization is the native PLAN→ACT transition on the same persistent Producer session; no additional canonical implementation prose is required.
- PR_COMPOSE uses the canonical terse PR-composition prose control and expects `pr_result`.

There is no independent implementation ReviewLoop in v1. Initial `implementation_result` leads to authoritative GitHub branch/head reconciliation and `PR_AUTHORIZATION`; formal repository review begins only after OpenOrc creates/reconciles the canonical PR and binds REVIEW to that exact PR/head. During PR remediation, a new `implementation_result` leads to GitHub head reconciliation and then a fresh REVIEW of the new exact PR/head.

For formal review, the outcome semantics are:

```text
ACCEPTED
→ no changes required before the exact subject advances technically

CHANGES_REQUESTED
→ revision/remediation required
→ one or more concrete findings
```

Failure to obtain a valid `review_result` because of timeout, provider/runtime failure, session loss, malformed output, or schema-invalid output is an operational/protocol failure, not a Reviewer outcome.

OpenOrc reacts to structured results, not to prose containing words such as "accepted" or "approved". Exact subject identity remains OpenOrc/GitHub-owned context and is not established by identifiers echoed by the agent.

For PR composition, OpenOrc sends the canonical `controls/pr_compose.md` interaction to the existing Producer session after `PR_AUTHORIZATION`. Workspace guidance may influence writing conventions where OpenOrc chooses to make it salient, but it cannot grant GitHub authority. The Producer returns only human-readable title/body through `pr_result`; it never performs the GitHub PR operation itself. OpenOrc deterministically ensures the published PR closes the Task issue, preserving an effective Producer-authored closing reference when present or appending one when absent.

Workflow-control realization is runtime-specific without changing OpenOrc workflow semantics. v1 records only demonstrated behavior: Cline IMPLEMENT requires the native PLAN→ACT transition and no extra prose. Future runtimes may realize the same semantic control differently when such a runtime is actually integrated; v1 does not invent speculative prompt requirements for them.

## 9.5 Formal response boundary

Formal interactions follow this boundary:

```text
OpenOrc formal request
↓
provider/runtime-native response
↓
Connection adapter
  extracts the agent response
  performs harmless syntax/presentation normalization
  parses the expected JSON object
  validates it against the expected v1 schema
↓
normalized typed OpenOrc result
↓
deterministic workflow logic
```

Harmless presentation normalization may include removing Markdown code fences around otherwise valid JSON.

The adapter must not infer semantic meaning from unstructured prose or invent missing fields. If a formal review request expects `review_result` and the agent returns only `Looks good to me`, the adapter returns a protocol failure rather than manufacturing `ACCEPTED`.

Runtime/provider events and machine facts are normalized separately where relevant.

The v1 formal response types remain runtime-independent OpenOrc semantic contracts. Using Cline Hub for both roles does not merge or remove them:

```text
session initialization
→ session_ready

Producer planning / plan revision
→ plan_result

Reviewer plan review / PR review
→ review_result

Producer implementation / PR remediation completion
→ implementation_result

Producer PR composition
→ pr_result
```

Owner ↔ Reviewer discussion is intentionally excluded from the formal response protocol.

## 9.6 `session_ready` v1

Returned after successful OpenOrc Task-session initialization:

```json
{
  "type": "session_ready",
  "schema_version": 1,
  "status": "READY"
}
```

Canonical v1 JSON Schema:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["type", "schema_version", "status"],
  "properties": {
    "type": {
      "const": "session_ready"
    },
    "schema_version": {
      "const": 1
    },
    "status": {
      "const": "READY"
    }
  }
}
```

OpenOrc already knows the Task, role, Connection, and external-session binding. The agent does not echo those identifiers back as protocol authority.

## 9.7 `plan_result` v1

Returned by the Producer for both an initial and revised implementation strategy:

```json
{
  "type": "plan_result",
  "schema_version": 1,
  "plan": "## Implementation strategy\n\nMarkdown plan here..."
}
```

Canonical v1 JSON Schema:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["type", "schema_version", "plan"],
  "properties": {
    "type": {
      "const": "plan_result"
    },
    "schema_version": {
      "const": 1
    },
    "plan": {
      "type": "string",
      "minLength": 1
    }
  }
}
```

The `plan` string may contain Markdown. That exact semantic content becomes the new `PlanRevision`.

## 9.8 `review_result` v1

Used for both planning review and commit-addressed PR review:

```json
{
  "type": "review_result",
  "schema_version": 1,
  "outcome": "CHANGES_REQUESTED",
  "summary": "The overall approach is sound, but one issue must be resolved.",
  "findings": [
    {
      "summary": "Rollback behavior is unspecified",
      "details": "Define the expected rollback behavior before the subject advances."
    }
  ]
}
```

Canonical v1 JSON Schema:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "type",
    "schema_version",
    "outcome",
    "summary",
    "findings"
  ],
  "properties": {
    "type": {
      "const": "review_result"
    },
    "schema_version": {
      "const": 1
    },
    "outcome": {
      "enum": ["ACCEPTED", "CHANGES_REQUESTED"]
    },
    "summary": {
      "type": "string",
      "minLength": 1
    },
    "findings": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["summary", "details"],
        "properties": {
          "summary": {
            "type": "string",
            "minLength": 1
          },
          "details": {
            "type": "string",
            "minLength": 1
          }
        }
      }
    }
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "outcome": {
            "const": "ACCEPTED"
          }
        }
      },
      "then": {
        "properties": {
          "findings": {
            "maxItems": 0
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "outcome": {
            "const": "CHANGES_REQUESTED"
          }
        }
      },
      "then": {
        "properties": {
          "findings": {
            "minItems": 1
          }
        }
      }
    }
  ]
}
```

Semantics:

```text
ACCEPTED
→ no findings

CHANGES_REQUESTED
→ one or more actionable findings

failure to obtain valid review_result
→ separate operational/protocol error path
```

OpenOrc binds the result to the exact PlanRevision or commit-addressed PR subject it supplied. The Reviewer does not echo Task IDs, PlanRevision IDs, PR IDs, SHAs, or similar identifiers as authoritative protocol fields.

## 9.9 `implementation_result` v1

Returned by the Producer when an implementation or PR-remediation run has completed semantically. For the initial implementation run, the result also identifies the Producer-created feature branch that contains the completed work:

```json
{
  "type": "implementation_result",
  "schema_version": 1,
  "status": "COMPLETED",
  "branch": "refactor/usage-analytics-123",
  "summary": "Implemented the requested change while preserving the existing external contract.",
  "changes": [
    "Extracted the capability into a transport-neutral service.",
    "Rewired the existing adapter to use the new service."
  ],
  "validation": [
    "Backend test suite passed.",
    "Static analysis passed."
  ],
  "notes": null
}
```

Canonical v1 JSON Schema:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "type",
    "schema_version",
    "status",
    "branch",
    "summary",
    "changes",
    "validation"
  ],
  "properties": {
    "type": {
      "const": "implementation_result"
    },
    "schema_version": {
      "const": 1
    },
    "status": {
      "const": "COMPLETED"
    },
    "branch": {
      "type": "string",
      "minLength": 1
    },
    "summary": {
      "type": "string",
      "minLength": 1
    },
    "changes": {
      "type": "array",
      "items": {
        "type": "string",
        "minLength": 1
      }
    },
    "validation": {
      "type": "array",
      "items": {
        "type": "string",
        "minLength": 1
      }
    },
    "notes": {
      "type": ["string", "null"]
    }
  }
}
```

`implementation_result` reports semantic completion. The Producer creates and semantically names the Task feature branch as part of the authorized implementation workflow and reports that branch name here; there is no separate OpenOrc branch-creation step. The reported branch is a routing/discovery claim. After the Producer pushes it, OpenOrc reconciles that branch through GitHub before recording it as the Task's canonical feature branch and before PR publication.

Runtime-local branches, commits, worktrees, and HEAD SHAs inside an Agent Runtime may support adapter mechanics or diagnostics, but they are not canonical committed repository state. GitHub is authoritative for the durable Task branch and committed head after push; OpenOrc obtains those facts through GitHub reconciliation rather than trusting a SHA merely because the Producer, runtime, or LLM reported it. PR state, CI/check state, and other GitHub-owned machine facts likewise come from authoritative GitHub reconciliation.

For PR-remediation runs, the Producer continues on the Task's already-bound canonical branch; the adapter/application layer must verify that the result still corresponds to that branch.

Runtime/provider/transport failure is not represented by inventing another semantic Producer status inside this schema.

---

## 9.10 `pr_result` v1

Returned by the Producer after `PR_AUTHORIZATION` when OpenOrc requests the human-readable content for the canonical pull request:

```json
{
  "type": "pr_result",
  "schema_version": 1,
  "title": "Extract usage analytics into capability services",
  "body": "## Summary\n\n...\n\nCloses #123"
}
```

Canonical v1 JSON Schema:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["type", "schema_version", "title", "body"],
  "properties": {
    "type": {
      "const": "pr_result"
    },
    "schema_version": {
      "const": 1
    },
    "title": {
      "type": "string",
      "minLength": 1
    },
    "body": {
      "type": "string",
      "minLength": 1
    }
  }
}
```

`title` and `body` are semantic presentation authored by the Producer. The body is ordinary GitHub-flavored Markdown and may naturally include issue references or closing keywords such as `Closes #123`.

The Producer does not return repository identity, Task/issue identity, branch names, PR numbers, commit SHAs, or other machine facts in `pr_result`. By this point OpenOrc already has the verified canonical Task branch and obtains commit/PR state from authoritative Git/runtime/GitHub sources.

`pr_result` is composition only. The Producer must not create, open, publish, close, merge, or otherwise mutate the pull request as part of this interaction. OpenOrc validates the result and performs the authorized GitHub PR operation itself.

Because the canonical PR represents one executable Task, OpenOrc must deterministically ensure that the published PR will close that Task's GitHub issue on successful merge. If the validated Producer-authored body already contains an effective GitHub closing reference for the Task issue, it may be preserved as-is; otherwise OpenOrc may append the required deterministic closing reference during publication. No redundant structured issue field is added to `pr_result`.

Runtime/provider/transport failure or schema-invalid PR content travels through the normal operational/protocol error path rather than being inferred or repaired semantically.

---

# 10. ReviewLoops and Planning

OpenOrc uses a reusable bounded ReviewLoop only where Producer and Reviewer can reliably observe the same review subject.

v1 purposes are:

```text
PLANNING
PR_REVIEW
```

There is no independent implementation ReviewLoop in v1.

The generic loop is:

```text
Producer
↓
reviewable subject
↓
Reviewer
├── ACCEPTED → advance
└── CHANGES_REQUESTED → findings back to Producer

operational/provider/protocol failure
→ adapter error path
→ deterministic retry/recovery policy
```

ReviewLoops always use the Task's existing Producer and Reviewer sessions.

OpenOrc owns:

- loop state;
- iteration count;
- max-iteration policy;
- routing to the correct Task sessions;
- exact review-subject identity;
- persisted structured Reviewer results/findings;
- workflow/audit records;
- termination and owner-gate creation when the limit is reached.

Agents own semantic judgment. Connected agent systems own conversational context. OpenOrc owns deterministic orchestration.

## 10.1 Reviewer outcomes

Canonical Reviewer outcomes are:

```text
ACCEPTED
CHANGES_REQUESTED
```

`ACCEPTED` means the exact current review subject is technically acceptable to advance to the next required human authority boundary. It does not authorize implementation, PR publication, or merge.

The Reviewer does not emit `FAILED` or `ESCALATED` in v1.

Operational/provider/session/protocol failures are separate adapter failures handled by retry/recovery policy.

## 10.2 Iteration limit and REVIEW_RESOLUTION

Every ReviewLoop has a maximum number of Reviewer evaluations. The default is:

```text
max_iterations = 5
```

The value is configurable at Workspace scope.

If the final allowed evaluation still returns `CHANGES_REQUESTED`, OpenOrc ends the automatic Producer ↔ Reviewer consensus loop and moves the Task to:

```text
WAITING_FOR_OWNER
→ gate = REVIEW_RESOLUTION
→ reason = MAX_ITERATIONS_REACHED
```

The exact current review subject, the Reviewer's unresolved findings, and ReviewIteration history are preserved. The Producer is no longer part of this exhausted ReviewLoop.

At this gate:

- `Discuss` continues free-form Owner ↔ Reviewer discussion in the existing Reviewer session;
- `Approve` is enabled and lets the Owner explicitly override the unresolved Reviewer objection for the exact current subject;
- `Cancel` ends and archives the OpenOrc Task;
- discussion itself does not change workflow state;
- no further automatic Producer ↔ Reviewer iteration occurs for that exhausted subject.

`Approve` is an Owner authority decision, not a fabricated Reviewer acceptance. The durable record preserves the final `CHANGES_REQUESTED`, the exhausted ReviewLoop, and the Owner's explicit `REVIEW_RESOLUTION` approval.

Approval resolves the disagreement and creates the normal next OwnerGate without silently granting that later authority:

```text
plan subject → IMPLEMENTATION_AUTHORIZATION
PR/head subject → MERGE_DECISION
```

The Owner still separately authorizes implementation or requests merge at those normal gates.

## 10.3 PlanRevision

Planning artifacts are versioned Producer PlanRevisions.

```text
Task
├── PlanRevision v1
├── PlanRevision v2
└── PlanRevision v3
```

Typical data may include:

```text
id
workspace_id
task_id
revision_number
producer_connection_id
content
repository_base_sha
created_at
```

`content` is the exact validated `plan` Markdown string returned by the Producer.

`repository_base_sha` records the committed repository context against which the plan was produced/reviewed for audit and diagnostics. It is not an automatic implementation invalidation guard. If the repository base advances after planning clearance or implementation authorization, the review-cleared PlanRevision remains valid; when implementation begins, the Producer works from the then-current repository state. Repository-specific conflict, rebase, update-branch, and merge requirements remain GitHub/repository policy concerns.

Each ReviewIteration references the exact PlanRevision reviewed.

When the Reviewer accepts a PlanRevision, OpenOrc:

1. records Reviewer acceptance of that exact revision;
2. preserves it as immutable audit history;
3. publishes that exact final plan to GitHub;
4. creates `IMPLEMENTATION_AUTHORIZATION` tied to that revision;
5. moves the Task to `WAITING_FOR_OWNER`.

If the planning ReviewLoop instead exhausts its limit and the Owner later approves `REVIEW_RESOLUTION`, the same exact PlanRevision is published and becomes eligible for `IMPLEMENTATION_AUTHORIZATION`, but the audit record retains the final Reviewer `CHANGES_REQUESTED` plus the Owner override rather than recording a false Reviewer acceptance.

Once either path clears planning, the Task never returns to `PLANNING`. A fundamental plan change requires cancellation/archive and a fresh Task.

There is no separate `AcceptedPlan` entity and no second mutable implementation-plan artifact derived from the review-cleared PlanRevision.

Intermediate planning/review chatter is not published to GitHub.

---

# 11. Commit-Addressed Repository Review

Producer and Reviewer may share physical runtime infrastructure, but they do not share Task working context. Git is the shared source of truth for code review. The Producer uses its own mutable Task working context; the Reviewer uses a separate review context that is prepared against the exact committed subject under review.

Any code submitted for independent review must first be committed, and the review result must be bound to the exact repository state reviewed.

```text
Producer works privately
↓
Producer commits/pushes
↓
OpenOrc records review subject:
  PR identity
  exact head SHA
↓
Reviewer evaluates committed state
↓
result bound to that PR + head SHA
```

Reviewer acceptance is valid only for that canonical PR at that exact head SHA. Before each formal repository review, the runtime/adapter must ensure that the existing Reviewer session's isolated working context resolves to that exact committed subject while preserving the same Reviewer `sessionId` and conversational history. The Cline adapter may rebuild/rebind the internal runtime instance against the prepared Reviewer worktree when necessary; that does not create a new OpenOrc Task session. A later commit changes the PR head, immediately makes prior acceptance stale, and triggers preparation/review of the new head. Movement of the PR's target/base branch alone does not invalidate OpenOrc Reviewer acceptance; GitHub remains authoritative for conflicts, update/rebase requirements, CI, branch protection, and merge eligibility. If the PR itself is rebased or otherwise updated, its head SHA changes and the ordinary staleness rule applies.

Textual PlanRevisions do not require a Git commit merely to participate in planning review, although planning occurs against known committed repository state.

A Reviewer must never be asked to infer or review private uncommitted Producer filesystem state.

---

# 12. Execution, Runtime Requests, Failure, and Recovery

## 12.1 Execution

An Execution is one bounded Producer runtime run/attempt for an executable Task inside the Task's already-established Producer session.

Possible data:

```text
id
workspace_id
task_id
task_agent_session_id
runtime_connection_id
runtime_execution_id
attempt
status
result_metadata
started_at
completed_at
failure_metadata
```

Execution is not an implementation phase and is not a conversational session boundary. Execution persistence does not treat runtime-local Git state or sandbox/worktree HEAD SHAs as canonical repository truth. If later implementation demonstrates a need for Execution-level committed-repository observations, those facts must have precise semantics and be populated from GitHub-reconciled committed state rather than copied from runtime-local Git state.

The normal v1 intent is one implementation run for a well-scoped Task. Retries, runtime recovery, or explicit continuation may create additional historical Execution records while continuing to use the same Producer `TaskAgentSession`.

Where the bound Agent Runtime exposes supported cancel/resume controls, the Owner may interrupt or resume runtime activity on the exact existing Task session without cancelling the OpenOrc Task. These controls are operational runtime controls, not lifecycle authorization gates and not free-form Owner ↔ Producer communication. They must preserve the same `TaskAgentSession`, external session identity, and conversational context. Their precise mapping onto Execution status/history depends on the concrete runtime semantics: a runtime may expose a resumable pause within one Execution or may end one runtime attempt and require a continuation Execution in the same TaskAgentSession. OpenOrc must not infer Task cancellation, session loss, or session replacement merely because the Owner used runtime cancel.

OpenOrc stores runtime state/results exposed by the adapter and the validated Producer `implementation_result` where applicable. It does not persist or reconstruct chain-of-thought, runtime-owned conversational context, or uncommitted filesystem state.

## 12.2 RuntimeRequest

A `RuntimeRequest` represents one scoped runtime-originated interaction that the OpenOrc adapter must explicitly service before the same bound session can continue. It exists only where the validated runtime contract requires OpenOrc participation; native runtime approvals/questions that are handled entirely inside the runtime's own UI/configuration are not promoted into OpenOrc workflow state.

The v1 semantic kind is:

```text
ACTION_APPROVAL
→ optional adapter-mediated approval only where the validated runtime contract requires OpenOrc participation
→ Owner approves or rejects that exact externally surfaced request
```

Conceptually:

```text
runtime session pauses on an interaction that requires OpenOrc participation
↓
adapter receives a correlated public request/callback
↓
OpenOrc persists RuntimeRequest(PENDING) and surfaces it to the Owner
↓
Owner provides the scoped response
↓
OpenOrc returns that response through the adapter to the exact pending runtime request
↓
same bound session resumes
```

The runtime decides when such an externally surfaced request is necessary. OpenOrc does not infer one from free-form model prose and does not invent a general Owner ↔ Producer chat channel. For Cline v1, native tool permissions/approvals remain Cline-owned unless Cloud #2 R2b proves that the public remote-session contract necessarily requires an OpenOrc-side callback/pass-through.

A RuntimeRequest is tied to the exact external approval/action identifier and Task/Producer session. The Owner response is a typed, correlated workflow control rather than Owner-initiated conversation. It may occur where the concrete runtime pauses a consequential action for approval. The Reviewer does not participate in Producer RuntimeRequests. Ordinary implementation ambiguity remains the Producer's responsibility to resolve autonomously; OpenOrc does not provide a general Owner-decision/question channel to the Producer in v1.

Runtime-specific request mechanics remain adapter concerns. Cline v1 does not assume that native tool approval belongs in OpenOrc: if the Cline Box/dashboard can service the native request, it stays entirely Cline-owned. Only a validated public remote-session callback that OpenOrc must service may be normalized into `RuntimeRequest`, preserving the same Task session identity and conversation.

## 12.3 Failure and recovery

Agent interactions and Producer execution may fail because of:

- temporary network errors;
- model/provider outage;
- rate limiting;
- insufficient provider funds;
- invalid credentials;
- runtime crash;
- Hub failure;
- OOM or disk/resource exhaustion;
- stalled execution;
- loss of the bound Producer or Reviewer session;
- protocol failure;
- authoritative GitHub source changes that invalidate the current Task attempt;
- unknown error.

Adapters normalize provider/runtime-specific failures sufficiently for deterministic OpenOrc policy.

External-operation handling in v1 is deliberately conservative:

```text
known success
→ continue normally

known failure
→ bounded automatic retry where policy permits
→ if still failing, BLOCKED

timeout / uncertain outcome
→ BLOCKED immediately
→ do not automatically repeat the uncertain operation
```

A timeout means OpenOrc does not know whether the external system applied the request; it does not reinterpret uncertainty as failure. Recovery after the hard stop is Owner-driven. The exact recovery action may be retry, correction, cancellation, starting different work, or another operation-specific action and does not need a universal v1 recovery workflow.

The same conservative rule applies to stale/concurrent workflow operations. Every workflow-changing operation is logically bound to the exact current subject or authority context that created it, such as a PlanRevision, OwnerGate, Execution, PR/head review subject, or equivalent current object. If a delayed job, retry, webhook consequence, or Owner action no longer matches that current subject/context, OpenOrc must not apply it. Automatic progression stops, the stale condition and context are surfaced, and recovery is Owner-driven.

If GitHub's authoritative issue requirements change after an OpenOrc Task attempt has begun, OpenOrc does not attempt semantic change analysis or silently adapt the existing plan/execution. The Task enters `BLOCKED` with the changed source of truth surfaced to the Owner. The supported v1 continuation is to cancel/archive that Task and start a fresh Task against the issue's new authoritative state.

Clearly transient known failures may receive bounded retries. If automatic progression cannot safely continue, the Task enters `BLOCKED` with a persisted reason and deterministic resume context.

Recovery context should be reconstructable from durable sources such as:

- GitHub issue requirements and relationships;
- review-cleared PlanRevision and review history;
- committed repository state;
- Execution attempt metadata;
- RuntimeRequest history;
- workflow state and block metadata.

OpenOrc does not promise reconstruction of uncommitted runtime-private state or private reasoning.

Unexpected role-session loss must not trigger silent session replacement. In v1 it is `AGENT_SESSION_LOST` unless the external system can restore the exact bound context. Deliberate emergency destruction of an OpenOrc-managed runtime is an explicit terminal-loss case for every affected bound session: those Task attempts cannot resume in place and continuation requires cancellation/archive followed by a fresh Task with fresh role sessions.

A role session must never be the sole durable record of workflow authority or engineering conclusions.

---

# 13. Workflow State Machine

The primary Task state machine describes the engineering lifecycle for executable Tasks. It deliberately does not absorb every review, retry, CI, RuntimeRequest, dependency, session, or runtime condition into one enum.

Primary states:

```text
READY_TO_PLAN
QUEUED
PLANNING
WAITING_FOR_OWNER
IMPLEMENTING
REVIEWING
BLOCKED
CANCELLED
COMPLETED
```

Composite GitHub issues do not have an executable OpenOrc Task and therefore do not traverse this state machine.

Canonical happy path when runtime capacity is immediately available:

```text
READY_TO_PLAN
↓
PLANNING
↓
WAITING_FOR_OWNER
  IMPLEMENTATION_AUTHORIZATION
↓
IMPLEMENTING
↓
WAITING_FOR_OWNER
  PR_AUTHORIZATION
↓
REVIEWING
↓
WAITING_FOR_OWNER
  MERGE_DECISION
↓
COMPLETED
```

No primary-state transition creates a new role session.

## 13.1 Eligibility and READY_TO_PLAN

A Task may enter `READY_TO_PLAN` only when:

- its authoritative GitHub issue exists and is open;
- GitHub does not currently report the issue as blocked by an unresolved issue dependency;
- applicable Workspace/Project policy permits work to begin.

Parent/sub-issue hierarchy does not participate in this eligibility predicate.

When the Owner starts an eligible Task, OpenOrc checks whether the underlying configured runtime Connection(s) have aggregate capacity for the Task's isolated role sessions. Multiple role bindings pointing at one runtime consume from that shared capacity pool.

```text
capacity available
→ idempotently establish required Producer/Reviewer Task sessions
→ PLANNING

capacity unavailable
→ QUEUED
```

`QUEUED` is normal backpressure, not failure. Queued Tasks are ordered FIFO by the time they entered `QUEUED`; when the required Owner-configured runtime Connection capacity becomes available, the oldest queued Task that can acquire all required role-session capacity advances automatically. Capacity accounting uses the configured OpenOrc Connection limit rather than a Cline-discovered slot count; exact scheduling mechanics remain implementation details.

Task role sessions persist for the Task lifetime once established, so ordinary later workflow transitions do not repeatedly return the Task to `QUEUED` merely to reacquire the same sessions.

## 13.2 PLANNING

Producer and Reviewer run the bounded planning ReviewLoop.

Review-cleared PlanRevision:

```text
Reviewer ACCEPTED
OR Owner approved REVIEW_RESOLUTION after loop exhaustion
↓
publish exact review-cleared plan to GitHub
→ WAITING_FOR_OWNER
→ IMPLEMENTATION_AUTHORIZATION
```

ReviewLoop limit reached with changes requested:

```text
→ WAITING_FOR_OWNER
→ REVIEW_RESOLUTION
```

Unrecoverable review/session/Connection failure:

```text
→ BLOCKED
```

## 13.3 WAITING_FOR_OWNER

`WAITING_FOR_OWNER` is the reusable lifecycle state for explicit human authority or intervention.

Initial OwnerGate types:

```text
IMPLEMENTATION_AUTHORIZATION
PR_AUTHORIZATION
MERGE_DECISION
REVIEW_RESOLUTION
```

At Reviewer-related gates, the Owner may discuss the Task with the existing Reviewer session. Discussion does not resolve the gate or change state.

### IMPLEMENTATION_AUTHORIZATION

Authorizes autonomous implementation of the exact review-cleared PlanRevision.

### PR_AUTHORIZATION

Authorizes creation/publication of the canonical PR after implementation reaches the Workspace's human decision point.

OpenOrc does not prescribe a universal E2E, staging, QA, or validation method before approval.

### MERGE_DECISION

Requests that GitHub merge the current reviewed PR head. GitHub remains authoritative on whether repository policy permits the merge.

### REVIEW_RESOLUTION

Requires Owner intervention after a bounded planning or PR ReviewLoop reaches its iteration limit with changes still requested. The automatic Producer ↔ Reviewer loop is finished for that exact subject. The Owner may Discuss with the existing Reviewer, explicitly Approve/override the unresolved objection, or Cancel. Owner approval is recorded as an override and advances to the normal next OwnerGate; it does not rewrite the Reviewer's last result as `ACCEPTED`.

## 13.4 IMPLEMENTING

The Producer implements the authorized Task in the existing Producer session.

The Task remains `IMPLEMENTING` through:

- active runtime execution;
- bounded retry/recovery;
- pending Producer/runtime `RuntimeRequest` pauses;
- explicit continuation after recoverable interruption.

There is no implementation Reviewer loop and no Owner ↔ Producer chat.

Validated completion moves the Task to:

```text
WAITING_FOR_OWNER
→ PR_AUTHORIZATION
```

Unsafe/unrecoverable continuation moves the Task to `BLOCKED`.

## 13.5 REVIEWING

A PR exists and Producer/Reviewer evaluate/remediate the proposed final repository change using the Task's existing sessions and committed Git state. Creation of the canonical PR, and later reconciliation of any changed canonical PR head, requires OpenOrc to prepare the Task's existing Reviewer session against the exact current PR/head SHA and issue an exact-subject REVIEW interaction expecting `review_result`. GitHub events notify OpenOrc of external head changes; OpenOrc owns the review trigger and routing rather than relying on the Reviewer runtime to subscribe to GitHub directly.

The Task remains `REVIEWING` through:

- Reviewer inspection;
- `CHANGES_REQUESTED`;
- Producer remediation;
- commit/push;
- re-review;
- GitHub CI running/passing/failing.

Reviewer acceptance of the current PR head moves the Task to:

```text
WAITING_FOR_OWNER
→ MERGE_DECISION
```

ReviewLoop limit exhaustion moves it to:

```text
WAITING_FOR_OWNER
→ REVIEW_RESOLUTION
```

A changed PR head invalidates prior acceptance.

## 13.6 BLOCKED

`BLOCKED` means automatic progression has stopped and human intervention or external correction is required.

Candidate reasons include:

```text
REVIEW_FAILURE
RUNTIME_FAILURE
AGENT_SESSION_LOST
CONNECTION_UNAVAILABLE
INVALID_CREDENTIALS
EXTERNAL_OPERATION_UNCERTAIN
STALE_OPERATION
GITHUB_SOURCE_CHANGED
RUNTIME_REQUEST_REJECTED
PR_CLOSED_UNMERGED
OWNER_ACTION_REQUIRED
UNKNOWN
```

Review iteration-limit exhaustion is not a `BLOCKED` reason; it uses `WAITING_FOR_OWNER / REVIEW_RESOLUTION`.

The block reason and persisted resume context determine the legal recovery destination. There is no universal `BLOCKED → X` transition.

## 13.7 CANCELLED

Cancellation intentionally ends OpenOrc orchestration for the Task. It is distinct from failure and preserves audit history. It is also distinct from an Owner using a runtime's supported cancel/stop control to interrupt current activity on an otherwise continuing Task session. Runtime activity cancel/resume leaves the OpenOrc Task and TaskAgentSession alive; Task cancellation is the terminal OpenOrc workflow action described here.

When active Producer runtime work exists, OpenOrc requests cancellation/abort through the runtime adapter. Cancellation never attempts to undo already-produced external Git state. Failure or uncertainty of the runtime abort is surfaced for inspection but does not resurrect the OpenOrc Task or keep the GitHub issue locked.

Cancellation does not implicitly close, delete, revert, or otherwise mutate external GitHub artifacts such as the issue, branch, commits, PR, or CI/check state.

`CANCELLED` is the terminal workflow outcome for that Task attempt. OpenOrc ends active use of its role-session bindings and archives/releases the Task as the current mapping for its GitHub issue, allowing a fresh Task with fresh role sessions to be started against that issue later. Archival/current-record mechanics are persistence concerns rather than another primary engineering workflow state.

## 13.8 COMPLETED

`COMPLETED` means GitHub has confirmed the canonical PR merged.

It is terminal. Later regressions or follow-up work create new Tasks rather than rewriting completed workflow history.

## 13.9 Canonical transition table

| From | Event / condition | To | Key effect / guard |
|---|---|---|---|
| eligible GitHub issue | synchronized/eligible | `READY_TO_PLAN` | issue open; GitHub does not currently report it blocked; hierarchy does not affect eligibility |
| `READY_TO_PLAN` | start requested; required runtime capacity unavailable | `QUEUED` | enqueue by queue-entry time; capacity exhaustion is not failure |
| `READY_TO_PLAN` | start requested; required runtime capacity available | `PLANNING` | idempotently establish Producer + Reviewer sessions |
| `QUEUED` | required runtime capacity becomes available and Task reaches FIFO eligibility | `PLANNING` | establish required isolated role sessions; oldest queued eligible Task first |
| `PLANNING` | PlanRevision accepted | `WAITING_FOR_OWNER` | publish exact plan; create `IMPLEMENTATION_AUTHORIZATION` |
| `PLANNING` | ReviewLoop limit reached | `WAITING_FOR_OWNER` | create `REVIEW_RESOLUTION`; preserve subject/findings/history; automatic Producer ↔ Reviewer iteration stops |
| `PLANNING` | unrecoverable review/session failure | `BLOCKED` | preserve resume context; no implicit session replacement |
| `WAITING_FOR_OWNER` | implementation authorized | `IMPLEMENTING` | authorization tied to exact PlanRevision; repository base may have advanced without invalidating plan |
| `IMPLEMENTING` | implementation completes; reported branch verified | `WAITING_FOR_OWNER` | bind canonical Task branch; record latest committed head; create `PR_AUTHORIZATION` for that exact head |
| `IMPLEMENTING` | unrecoverable condition/session loss | `BLOCKED` | record reason + resume context |
| `WAITING_FOR_OWNER` | PR authorized; preflight head still current; valid `pr_result`; PR created; reconciled PR head still matches authorized head | `REVIEWING` | Producer composes title/body; OpenOrc creates canonical PR, records/reconciles returned PR identity/head, then dispatches review only for the exact authorized head |
| `WAITING_FOR_OWNER` | PR-authorized head changed before PR creation | `BLOCKED` | authorization is stale; no PR is created; surface exact old/new subject and await Owner recovery |
| `WAITING_FOR_OWNER` | PR created but immediate reconciliation finds head differs from authorized head | `BLOCKED` | preserve/record created PR; reason = `STALE_OPERATION`; do not dispatch Reviewer until Owner recovery |
| `WAITING_FOR_OWNER` | implementation changes requested | `IMPLEMENTING` | continuation/new Execution in existing Producer session/canonical branch |
| `REVIEWING` | Reviewer accepts current PR + exact head SHA | `WAITING_FOR_OWNER` | create `MERGE_DECISION` bound to that reviewed head |
| `REVIEWING` | PR head changes / new commit | `REVIEWING` | prior acceptance becomes stale; review new head |
| `REVIEWING` | target/base branch moves without PR-head change | `REVIEWING` | Reviewer acceptance is not invalidated solely by base movement; GitHub owns rebase/conflict/merge policy |
| `REVIEWING` | ReviewLoop limit reached | `WAITING_FOR_OWNER` | create `REVIEW_RESOLUTION` |
| `REVIEWING` | findings / CI changes | `REVIEWING` | continue commit-addressed review/remediation as appropriate |
| `REVIEWING` | unrecoverable review/session failure | `BLOCKED` | preserve reason + resume context |
| any nonterminal state | external operation times out / outcome uncertain | `BLOCKED` | do not automatically repeat uncertain operation; surface context for Owner recovery |
| any nonterminal state | delayed/concurrent workflow operation no longer matches current subject/gate | `BLOCKED` | reject stale effect; surface context for Owner recovery |
| `WAITING_FOR_OWNER` | Owner ↔ Reviewer discussion | `WAITING_FOR_OWNER` | advisory only; discussion itself never resolves a gate |
| `WAITING_FOR_OWNER` | `REVIEW_RESOLUTION` approved for plan subject | `WAITING_FOR_OWNER` | record Owner override; create `IMPLEMENTATION_AUTHORIZATION`; no further review iteration for exhausted subject |
| `WAITING_FOR_OWNER` | `REVIEW_RESOLUTION` approved for PR/head subject | `WAITING_FOR_OWNER` | record Owner override; create `MERGE_DECISION`; no further review iteration for exhausted subject |
| any active Producer phase | runtime emits scoped action-approval request | same primary state | create `RuntimeRequest`; pause exact Producer session until Owner response |
| any nonterminal Task | authoritative GitHub issue requirements change | `BLOCKED` | reason = `GITHUB_SOURCE_CHANGED`; continuation requires cancel/archive + fresh Task |
| `WAITING_FOR_OWNER` | merge requested with exact expected reviewed/overridden head; GitHub succeeds | `COMPLETED` | record merge; end active role sessions |
| `WAITING_FOR_OWNER` | merge requested; GitHub reports expected-head mismatch | `BLOCKED` | merge decision is stale; reconcile current PR/head and surface exact old/new subject for Owner recovery |
| `WAITING_FOR_OWNER` | merge requested; GitHub explicitly rejects for another repository-policy/state reason | `WAITING_FOR_OWNER` | surface authoritative GitHub error; known failure policy may apply where safe |
| `WAITING_FOR_OWNER` | remediation requested | `REVIEWING` | resume PR review/remediation |
| any nonterminal Task state | explicit cancellation | `CANCELLED` | request runtime abort if active; leave GitHub artifacts unchanged; archive/release current issue mapping |
| any PR-associated nonterminal state | GitHub reports merged | `COMPLETED` | external merge is authoritative |
| any PR-associated nonterminal state | GitHub reports closed unmerged | `BLOCKED` | reason = `PR_CLOSED_UNMERGED` |

---

# 14. OwnerGates, Human Validation, PR, CI, and Merge

## 14.1 OwnerGate vs RuntimeRequest

These are distinct concepts:

```text
OwnerGate
= permission/decision for the Task lifecycle to cross a consequential boundary
  or explicit Owner intervention after bounded review exhaustion

RuntimeRequest
= one optional scoped runtime-originated interaction that the adapter must service
  requiring an Owner response before the exact same bound session continues
```

Possible OwnerGate data:

```text
id
workspace_id
task_id
type
status
subject_type
subject_id / subject_sha
reason / context
created_at
resolved_at
resolved_by
```

Candidate statuses:

```text
PENDING
APPROVED
REJECTED
CANCELLED
```

Resolved gates are historical and are not recycled. If a Task later reaches the same gate type again, OpenOrc creates a new OwnerGate.

Canonical gate behavior:

```text
IMPLEMENTATION_AUTHORIZATION
  approve → IMPLEMENTING
  cancel / decline → CANCELLED + archived

PR_AUTHORIZATION
  subject → exact latest committed Producer head tracked by OpenOrc
  approve → request/validate Producer `pr_result` → preflight exact head → create PR → immediately reconcile PR head
           matching authorized head → REVIEWING
           mismatched post-create head → BLOCKED / STALE_OPERATION / Owner recovery
  head changes before PR creation → authorization stale → BLOCKED / Owner recovery
  request implementation changes → IMPLEMENTING
  cancel → CANCELLED + archived

MERGE_DECISION
  merge request includes exact reviewed/Owner-overridden head as GitHub expected SHA
  merge requested + GitHub succeeds → COMPLETED
  expected-head mismatch → BLOCKED / stale decision / Owner recovery
  other GitHub rejection → remain WAITING_FOR_OWNER
  request remediation → REVIEWING
  cancel → CANCELLED + archived

REVIEW_RESOLUTION
  Discuss → existing Reviewer session, no state change
  Approve → explicit Owner override of unresolved Reviewer objection for exact subject
            plan subject → create IMPLEMENTATION_AUTHORIZATION
            PR/head subject → create MERGE_DECISION
  Cancel → CANCELLED + archived
```

There is no transition back to `PLANNING` after a PlanRevision has cleared planning. If the Owner concludes that the approved/review-cleared plan itself must fundamentally change, the v1 path is cancellation/archive followed by a fresh Task.

`REVIEW_RESOLUTION` approval never changes the historical Reviewer result to `ACCEPTED`. The final `CHANGES_REQUESTED`, iteration exhaustion, Owner discussion, and Owner override remain distinct durable facts.

Implementation authorization references the exact review-cleared PlanRevision. `PR_AUTHORIZATION` references the exact latest committed Producer head OpenOrc presents to the Owner after implementation. A head change detected before PR creation makes the authorization stale; a mismatch discovered by immediate post-create PR reconciliation likewise becomes a stale operation and blocks before review. Merge decision is valid only for the current reviewed or Owner-overridden PR/head state, and the GitHub merge request carries that exact head SHA as its expected SHA.

## 14.2 Human validation before PR authorization

OpenOrc does not prescribe a universal E2E, staging, QA, or validation state between implementation completion and PR creation.

```text
IMPLEMENTING
↓
WAITING_FOR_OWNER
  gate = PR_AUTHORIZATION
```

A Workspace may require whatever validation it considers appropriate before approval. The Owner's PR authorization applies to the exact latest committed Producer head OpenOrc is tracking and presenting at that gate, not merely to the Producer's prose `implementation_result`.

For Babelbeez, the current reference practice is manual local product-level E2E after fetching the branch. That is a Workspace practice, not a universal OpenOrc state-machine requirement.

## 14.3 PR review and merge semantics

After implementation completion and Producer push, OpenOrc treats the Producer-reported branch as a routing/discovery claim, reconciles it through GitHub, records the GitHub-confirmed Task branch as the canonical feature branch, and records the exact GitHub-confirmed committed head presented at `PR_AUTHORIZATION`. Runtime-local Git state does not establish either workflow-authoritative fact.

After `PR_AUTHORIZATION`:

1. The Owner authorizes PR creation for that exact committed head.
2. OpenOrc issues the fixed PR-composition workflow control to the existing Producer session, with optional Workspace guidance where useful.
3. Producer returns a validated `pr_result`; it does not execute GitHub PR operations.
4. Immediately before publication, OpenOrc verifies that the authorized committed head is still the current head of the canonical Task branch. If it changed, the authorization is stale and automatic progression stops for Owner recovery.
5. OpenOrc ensures the published PR will close the Task's GitHub issue on successful merge, preserving an effective Producer-authored closing reference when already present or appending one deterministically when absent.
6. OpenOrc creates the canonical PR through GitHub API on behalf of the authenticated Owner, using that Profile's GitHub App user-to-server authorization plus authoritative repository/branch/Task state and the validated PR content. GitHub's PR-creation API is branch-addressed and does not provide an atomic expected-head-SHA guard.
7. Immediately after creation, OpenOrc records the returned PR identity and reconciles its authoritative head SHA. If the created PR head differs from the exact head authorized by the Owner, the operation is stale: OpenOrc does not dispatch review, preserves the created GitHub artifact, and moves the Task to `BLOCKED` for Owner recovery.
8. If the reconciled PR head still matches the authorized head, the Task enters `REVIEWING`.
9. OpenOrc prepares the existing Reviewer session against that exact canonical PR/head SHA and issues an exact-subject REVIEW interaction expecting `review_result`.
10. Reviewer evaluates that exact committed subject and returns a validated `review_result`.
11. Producer remediates requested changes privately on the canonical Task branch.
12. Producer commits/pushes new state.
13. GitHub notifies OpenOrc of the changed PR head; OpenOrc reconciles authoritative PR/head state, makes any prior Reviewer acceptance stale, prepares the existing Reviewer session against the new exact head, and issues a fresh exact-subject REVIEW interaction expecting `review_result`.
14. Acceptance of the current PR/head creates `MERGE_DECISION` for that exact reviewed head.
15. Human requests merge. OpenOrc invokes GitHub with that exact reviewed (or explicitly Owner-overridden) head SHA as the expected merge SHA, so a concurrently changed PR head cannot satisfy the stale merge decision.
16. GitHub enforces repository policy and performs or rejects the merge.
17. Task becomes `COMPLETED` only after GitHub confirms merge.

The PR-creation preflight/postflight reconciliation is deliberate. GitHub does not offer compare-and-create semantics for pull-request creation, so OpenOrc closes the small race window by validating immediately before creation and reconciling immediately after it. It does not invent a shadow branch or distributed lock to pretend the external API is atomic.

Movement of the PR target/base branch alone does not invalidate OpenOrc Reviewer acceptance. Repository-specific update-branch, conflict, rebase, CI, branch-protection, and merge requirements remain GitHub policy. If the PR itself is rebased or otherwise updated, its head SHA changes and the normal re-review rule applies.

GitHub owns:

- CI/check execution and status;
- branch protection;
- required checks/reviews;
- merge queue/policy;
- merge eligibility;
- actual merge operation.

GitHub may expose CI state through modern Checks and through commit statuses. OpenOrc reads the relevant check runs/check suites and combined commit-status state for the exact current PR head where applicable, then presents that GitHub-owned state without turning it into a second OpenOrc merge-policy engine.

OpenOrc owns:

- requesting and validating Producer-authored PR title/body after `PR_AUTHORIZATION`;
- deterministically ensuring the canonical PR closes its Task issue on successful merge;
- creating/publishing the canonical PR through GitHub API;
- displaying Reviewer state;
- displaying GitHub CI/check state;
- presenting the human merge decision;
- invoking GitHub when merge is requested;
- surfacing GitHub's result/error;
- transitioning to `COMPLETED` after confirmed merge.

OpenOrc does not duplicate merge guards already enforced by GitHub.

If GitHub rejects a merge request, the Task remains `WAITING_FOR_OWNER` and the authoritative GitHub error is surfaced.

External transitions remain authoritative:

```text
PR merged directly in GitHub
→ OpenOrc observes event
→ COMPLETED
```

```text
PR closed without merge
→ OpenOrc observes event
→ BLOCKED / PR_CLOSED_UNMERGED
```

GitHub hierarchy, dependency, and issue-requirement changes are authoritative observations, but only dependency blocking and issue requirements affect workflow eligibility. A hierarchy change alone never blocks or invalidates a Task. If GitHub newly reports the issue as blocked before execution begins, automatic progression stops. If the authoritative issue requirements change after an OpenOrc Task attempt has begun, the Task becomes `BLOCKED / GITHUB_SOURCE_CHANGED`; OpenOrc informs the Owner rather than attempting semantic reconciliation. The supported v1 continuation is cancellation/archive of that Task and creation of a fresh Task against the updated GitHub issue.

GitHub webhooks are the primary inbound notification mechanism for relevant external changes. A webhook signals that authoritative GitHub state may have changed; it does not replace GitHub as the source of truth. OpenOrc validates the webhook signature, deduplicates deliveries by GitHub delivery identity, reconciles the affected current state through the GitHub API where necessary, applies deterministic workflow consequences, and emits durable/live OpenOrc events as appropriate.

GitHub does not automatically redeliver failed webhook deliveries. OpenOrc therefore cannot make successful webhook delivery the only way authoritative state advances. The GitHub integration must have a recovery/reconciliation path capable of detecting or recovering from missed deliveries and re-reading current authoritative state. Exact delivery-history, redelivery, and/or periodic reconciliation mechanics remain an implementation decision. Reconciliation must tolerate replay, missed delivery, and out-of-order delivery.

Adopting an already-open PR as a new OpenOrc Task is a plausible post-v1 capability but is not part of v1 issue-backed intake.

---

# 15. Adapter, Application, Worker, and Delivery Semantics

OpenOrc separates workflow semantics, asynchronous execution machinery, and external-system transport.

Canonical call chain:

```text
RQ job / FastAPI request
↓
OpenOrc application service
↓
workflow/domain checks + durable state
↓
Connection / Task-session operation
↓
adapter
↓
external runtime/provider
```

Return path:

```text
external runtime/provider
↓
adapter
├── validated OpenOrc protocol object
└── normalized operational/protocol error
↓
OpenOrc application service
↓
deterministic workflow consequence
↓
Postgres + follow-on job/event where required
```

Workers do not orchestrate the product lifecycle themselves and do not talk directly to external providers/runtimes. An RQ worker executes an OpenOrc application operation using the same application/domain code used by the API.

The adapter owns only the external boundary:

- provider/runtime-native transport and session mechanics;
- extraction of the agent response;
- harmless syntax/presentation normalization;
- parsing and schema validation of formal responses;
- normalization of surfaced timeout, provider, authentication, session-loss, transport, and protocol failures.

The application service owns workflow meaning, current-subject validation, authority, state transitions, retry policy, and durable effects.

## 15.1 Message delivery and idempotency

OpenOrc does not assume the LLM or agent runtime provides semantic idempotency. Sending the same logical request twice may produce the same response, a different valid response, or an invalid/unstructured response. The model is therefore not part of the idempotency boundary.

For outbound Task-session messages, the adapter boundary is responsible for preventing blind duplicate dispatch. OpenOrc does not require every runtime to expose a distinct transport-level acknowledgement primitive; a concrete adapter may establish delivery/completion state from a synchronous send result, persisted session messages, runtime events, an explicit acknowledgement, or another supported authoritative mechanism.

The required v1 distinction is:

```text
OpenOrc dispatch D
↓
adapter checks durable dispatch/reconciliation state
├── already known delivered/completed
│   → do not send D again
├── known not delivered / explicit pre-delivery failure
│   → bounded retry where concrete adapter policy permits
└── outcome uncertain
    → reconcile through authoritative runtime/session state where possible
       ├── delivery/completion confirmed
       │   → record and continue
       ├── non-delivery confirmed
       │   → bounded retry where policy permits
       └── still uncertain
           → BLOCKED
           → do not automatically resend
```

A known successful dispatch/completion does not imply semantic idempotency inside the LLM. Conversely, timeout or connection loss must not be interpreted automatically as non-delivery. The adapter must reconcile if the runtime exposes enough persisted session/history/event state; if uncertainty remains, OpenOrc blocks rather than guessing whether resending would duplicate the request.

This rule governs duplicate **logical dispatch from OpenOrc to the runtime**. Qualified bridge/transport loss does not cancel Hub-accepted work. Reconcile before retrying; fresh-client plain subscriptions do not replay an existing mid-flight stream on `0.0.90`, while the original surviving client auto-resumed. Read public record/transcript/usage/history together with durable OpenOrc dispatch state. Persisted transcript and usage may lag to turn boundaries. If acceptance/completion or rebuild construction remains uncertain, block rather than replay.

This delivery rule is separate from session-creation idempotency. Session creation is idempotent for `(Task, role)`; message dispatch is protected by OpenOrc/adapter dispatch state and runtime reconciliation rather than by expecting the LLM to recognize duplicates or by requiring one universal acknowledgement API.

The exact durable dispatch key/table/outbox representation remains an implementation decision.

## 15.2 Live event delivery and runtime telemetry

OpenOrc owns event semantics independently of any particular delivery transport. In v1 it exposes browser-facing live updates through authorized Server-Sent Events (SSE); SSE is a delivery projection of OpenOrc events, not the event contract or a source of workflow authority. Supabase Realtime is not part of the OpenOrc v1 live-update architecture.

Two event classes remain distinct:

```text
OpenOrc events/state changes
→ owned by OpenOrc
→ durable when they have workflow/audit meaning
→ projected to authorized consumers through delivery transports
→ v1 live-client transport: SSE
→ future asynchronous transport may include outbound webhooks

runtime telemetry
→ owned by the connected runtime
→ optionally exposed through an adapter
→ surfaced through OpenOrc to authorized clients
→ not automatically persisted as OpenOrc workflow history
```

A future outbound-webhook surface may deliver selected OpenOrc event projections to configured external consumers without changing workflow/domain semantics. Such webhooks are notifications, not a command or authority surface; a consumer that needs to inspect state or perform an action uses an authenticated OpenOrc command/query surface. Phase 2 does not require outbound webhook delivery itself, but the event model must not couple event meaning to SSE or the Vue client.

OpenOrc should consume native runtime event/snapshot capabilities where available rather than duplicating high-frequency telemetry into Postgres merely to retransmit it. Examples include runtime/session status, assistant content updates, tool activity, usage, notices, completion, and runtime errors.

Only telemetry that acquires independent OpenOrc workflow/audit meaning becomes durable OpenOrc state or a `WorkflowEvent`. A live telemetry stream is not a second source of workflow authority.

OpenOrc application data, workflow commands, and runtime telemetry never depend on the browser calling an Agent Runtime or Reviewer provider directly. OpenOrc mediates those paths and their authorization. The UI may deliberately link the Owner to an external system's own native interface for direct inspection or management; following such a link is outside the OpenOrc application/control path. Exact internal fan-out from API/worker processes to SSE clients remains an implementation decision.

## 15.3 Background jobs

RQ is the v1 asynchronous execution mechanism, not canonical workflow state.

Examples include:

- synchronize GitHub issue/hierarchy/dependency state;
- establish Task role sessions when planning begins;
- start planning;
- review a PlanRevision;
- resume planning after changes requested;
- dispatch implementation;
- relay Owner ↔ Reviewer discussion;
- process runtime completion/failure/action-approval events;
- perform bounded retry/recovery;
- request/validate PR composition from the existing Producer session;
- create PR and dispatch initial PR review against its exact head;
- reconcile changed PR heads from GitHub notifications and dispatch fresh PR review;
- dispatch PR remediation.

Durable workflow state lives in Postgres. Lost/retried jobs must not duplicate workflow-changing external side effects, replace sessions, or corrupt Task state.

Before performing an asynchronous operation, the application service verifies that the Task and exact subject/authority context are still current. If a delayed/replayed operation no longer matches the current PlanRevision, OwnerGate, Execution, PR/head subject, or equivalent bound context, it must not apply the stale effect. OpenOrc stops automatic progression, persists/surfaces the stale condition, and waits for Owner-driven recovery rather than guessing.

## 15.4 Runtime abstraction and Cline boundary

The universal agent-session contract remains `create_session` + `send`. Cline-specific runtime controls are additional `ClineAdapter` concerns and do not become core OpenOrc session semantics merely because Cline supports or requires them.

`ClineAdapter` may expose only the additional operations OpenOrc actually needs, such as:

- Owner-triggered cancel/resume of current runtime activity on an exact bound Task session, preserving that session identity/context;
- Task-cancellation abort of active Producer work as a separate terminal workflow concern;
- scoped RuntimeRequest responses;
- execution/runtime events and session-scoped telemetry;
- runtime/session snapshots where supported;
- execution inspection;
- supported Cline Hub management/session deep links where available.

OpenOrc v1 consumes Cline through Cline's official SDK rather than implementing or maintaining the Cline Hub wire protocol directly. The architectural integration path is:

```text
OpenOrc application/domain
↓
Python ClineAdapter
↓
internal language-neutral ClineSdkBackend boundary
↓
v1 Node/TypeScript SDK bridge
  long-lived local subprocess
  JSON-RPC 2.0 over stdin/stdout
↓
official @cline/sdk
↓
public `ClineCore` remote client
→ same persistent managed Cline Hub/Box
```

The Node/TypeScript bridge is a language-compatibility shim, not an OpenOrc service or workflow layer. It is packaged with the OpenOrc control-plane processes that need Cline access and is started/lifecycle-managed locally by Python. It has no independent network endpoint, service discovery, authentication boundary, scaling identity, database, or durable state. A Python API or worker process may lazily own a bridge subprocess as needed; failure of that subprocess must not imply replacement of the external Task session.

The bridge owns only SDK-facing mechanics:

- invoking the official Cline SDK;
- translating SDK-native requests/results/events to and from plain JSON;
- maintaining the SDK client's connection lifecycle to the supported Cline remote-runtime surface;
- forwarding required SDK session events, usage and observable state; the qualified v1 contract registers no native approval/question callbacks.

The bridge must not own Tasks, PlanRevisions, ReviewLoops, OwnerGates, OpenOrc retry/idempotency policy, formal OpenOrc response validation, GitHub operations, workflow transitions, authorization, or durable workflow state. Those remain in Python application/domain code and `ClineAdapter`.

`ClineAdapter` depends on an internal `ClineSdkBackend`-style contract rather than on Node-specific process details. The v1 implementation of that contract uses the local SDK bridge. If Cline later publishes a suitable native Python SDK, OpenOrc should replace the bridge-backed implementation with an in-process Python implementation behind the same internal boundary; this migration must not require changes to the workflow/domain model, Task-session contracts, persistence, API, or higher-level adapter semantics.

OpenOrc must use the highest stable official SDK/runtime abstractions that satisfy the validated contracts. `@cline/sdk` / `ClineCore` is the primary v1 runtime-session surface; lower-level `Agent` / `AgentRuntime` APIs are not OpenOrc's integration layer. Provider/model selection is opaque role configuration; no catalog package or API is required. OpenOrc avoids:

- reimplementing Cline Hub's wire protocol;
- forking Cline internals;
- undocumented file dependencies;
- custom model/tool/agent loops;
- custom Cline worker management;
- mirroring Cline-native plugins, MCP, skills, tools, sub-agents/teams, provider credentials, or permission configuration into OpenOrc merely because the SDK exposes them.

Cline integration work is documentation-first: read the current official SDK overview, ClineCore guide, API reference, Events, Tools, Plugins, architecture, permission-handling, and production guides before source archaeology or live probing. Documentation defines the candidate public contract; targeted live qualification is reserved for pinned-version behavior, the remote Cline Box boundary, or documented ambiguities.

For each Task, `ClineAdapter` must preserve the exact Producer and Reviewer contexts independently across the Task lifetime. Producer sessions preserve context across planning, implementation, retries, and PR remediation; Reviewer sessions preserve context across planning review, Owner discussion, and PR review. Each Task/role receives an isolated runtime working context even when both roles share one Hub. The Producer context is mutable and owns the Task feature branch. The Reviewer context is non-authoritative, remains in Cline PLAN mode, and must be prepared to the exact committed review subject before formal review. Concrete clone/worktree/sandbox/checkout mechanics for presenting that exact committed subject remain runtime-specific. OpenOrc does not add a parallel Reviewer tool-permission layer on top of PLAN mode. The Producer creates and semantically names one feature branch for the Task as part of the authorized implementation workflow. On implementation completion and Producer push, the adapter/application path treats the branch reported in `implementation_result` as a routing/discovery claim and reconciles it through GitHub before OpenOrc binds the GitHub-confirmed branch as the Task's canonical branch. Runtime-local Git state, including sandbox/worktree HEAD SHAs, may support adapter mechanics or diagnostics but does not establish canonical committed repository truth. Later remediation continues on the canonical branch.

The completed R-series establishes CLI `3.0.68` / Core and SDK `0.0.90` as the supported baseline. Core pins the actual SDK dependency/lockfile in the bridge implementation; Cloud pins its CLI distribution and disables native auto-update on managed launches. The CLI owns managed Hub lifecycle/admin operation and is not a Task execution intermediary. No helper/second-Hub topology, private protocol, native credential file, or lower-level AgentRuntime API belongs in the Core contract.

Qualified `abort` interrupts an in-flight turn (finish reason `aborted`) and allows later turns on the same session. `stop` releases runtime incarnation and finalizes bookkeeping while retaining the public transcript; it does not prove session loss. Continuation is a subsequent authorized ordinary interaction after reconciliation, not automatic replay. Public finish reasons are `completed`, `max_iterations`, `aborted`, `mistake_limit`, and `error`; only valid formal OpenOrc results establish semantic success.

Public record status is Hub bookkeeping, not workflow authority or TaskAgentSession lifecycle. A settled-session Hub restart preserves records/history/transcripts but resets accumulated usage and makes the session non-addressable until same-ID reconstruction. Usage continuity holds only within one Hub lifetime. A fresh client recovers using public reads, not event-replay assumptions. After Hub lifecycle events, the runtime owner supplies current endpoint/control credentials through the existing Connection/auth seam; private Cloud credential discovery is not a Core API. `hub_connect_failed` does not distinguish invalid credentials from unreachability. `hub_connection_closed` with close code `1006` is a call-level loss signal, without a session-event equivalent. Deleted-session reads and missing records demonstrate genuine loss; deletion is not claimed to be the only possible loss condition. Unknown send/start acceptance or incomplete transcript remains uncertain and blocks automatic replay. Mid-turn Hub restart was not qualified and is not promised transparent recovery.

Future upgrades read current official SDK documentation first and run only affected representative remote compatibility checks, then update supported pins and the R5 decision ledger deliberately. Package-release notifications do not authorize upgrades. D7 validates the completed Core stack against the fixed baseline, without redoing the research archive or adding a generic certification framework.

## 15.5 GitHub adapter reconciliation boundary

The GitHub integration is a state-reconciling external adapter, not an event-driven second source of truth. Its v1 boundary preserves these semantics:

- inbound webhook deliveries are signature-validated and deduplicated by GitHub delivery identity before workflow consequences are considered;
- relevant deliveries trigger authoritative API reads/reconciliation of the affected issue, relationship, branch/commit, PR/head, CI/check, or merge state rather than trusting the webhook payload as final state;
- missed webhook delivery cannot permanently strand workflow state because an explicit reconciliation/recovery path exists independently of successful delivery;
- stable GitHub object identifiers and exact commit SHAs are retained wherever needed for deterministic persistence and reconciliation;
- PR publication is preflight-checked against the authorized branch head, then immediately postflight-reconciled because GitHub PR creation does not support an atomic expected-head condition;
- a post-create head mismatch records the created PR but stops progression as a stale operation before Reviewer dispatch;
- merge requests include the exact reviewed or Owner-overridden PR head SHA as GitHub's expected merge SHA;
- CI/check projection reads GitHub's check-run/check-suite and commit-status state for the exact relevant head as applicable, while GitHub remains authoritative for merge policy.

The application/domain layer decides the workflow consequence of reconciled facts. The GitHub adapter owns API/webhook transport and normalization, not Task-state semantics.

## 15.6 Fake/test adapters

Fake Agent Runtime adapters and role behaviors should exist early so workflow tests do not depend on live inference, remote VMs, Cline Hub, or model inference.

They must deterministically exercise:

- readiness handshake success/failure;
- formal protocol validation;
- session isolation;
- repeated session-creation calls;
- dispatch reconciliation/deduplication without blind replay after uncertain sends;
- surfaced adapter errors;
- loss of bound sessions;
- cross-Task contamination protections.

---

# 16. Workflow Events and Subordinate State

OpenOrc maintains an append-oriented durable workflow event stream for important transitions and decisions. This is distinct from the browser-facing SSE transport and from optional high-frequency runtime telemetry: not every live event becomes a persisted `WorkflowEvent`.

Possible event types include:

```text
task_created
agent_session_created
agent_session_bound
agent_session_lost
planning_started
plan_revision_created
plan_reviewed
review_limit_reached
plan_revision_revised
plan_ready
implementation_authorized
execution_started
execution_completed
execution_failed
runtime_request_created
runtime_request_resolved
runtime_request_cancelled
retry_started
task_blocked
owner_gate_created
owner_gate_resolved
owner_reviewer_discussion_message
workspace_configuration_changed
pr_created
pr_reviewed
task_relationship_synced
task_dependency_synced
pr_head_changed
merge_requested
merge_rejected_by_github
pr_merged
task_cancelled
task_completed
```

Possible logical actors:

```text
owner
openorc
producer
reviewer
runtime
github
```

Logical actor identity is preserved independently of infrastructure credentials.

Exact event schema remains an implementation decision.

Primary Task state remains intentionally small. Subordinate state may use shapes such as:

```text
ReviewLoop:
PENDING
PRODUCING
REVIEWING
CHANGES_REQUESTED
ACCEPTED
LIMIT_REACHED
FAILED

Execution:
QUEUED
RUNNING
PAUSED
PAUSED_FOR_APPROVAL
SUCCEEDED
FAILED_TRANSIENT
FAILED_FINAL
CANCELLED

RuntimeRequest:
PENDING
RESOLVED
EXPIRED
CANCELLED
```

Retry/recovery, RuntimeRequests, CI, GitHub dependency state, and session health remain subordinate/external conditions rather than new primary Task states.

---

# 17. Application Stack and Repository Structure

OpenOrc intentionally follows the same core application stack conventions as Babelbeez. The standard language runtimes are Python for the control-plane application and **Node 24** for JavaScript/TypeScript tooling. Node 24 is an existing OpenOrc stack dependency independent of Cline: it powers the Vite/frontend toolchain and also hosts the thin Cline SDK bridge described in §15.4. The Cline integration therefore adds an SDK package and compatibility shim, not a new language runtime or separately operated service.

## 17.1 Frontend

```text
Vue 3
Vite
TypeScript
TanStack Vue Query
Pinia where appropriate
Vue Router
Vitest
```

The UI/component library is intentionally **TBD**. It will be selected explicitly when substantive frontend design and implementation begin; ordinary feature, bootstrap, or dependency work must not implicitly establish one.

TanStack Vue Query owns server-derived state. Pinia is reserved for genuine client/application state. TanStack Vue Query is also the canonical frontend seam for observing API/query/mutation failures: shared query/mutation error handling decides which failures are ordinary product outcomes and which are operationally interesting enough to report, rather than individual components independently logging the same failure. The product SPA remains independently buildable and does not assume same-origin API behavior. The reference Cloud deployment packages it as the static-site component of the same DigitalOcean App Platform application as the API and worker; this packaging choice does not couple its runtime behavior to FastAPI.

## 17.2 Backend

```text
Python
FastAPI
Pydantic
pytest
Ruff
Pyright
```

Transport and process entrypoints remain thin. HTTP routers and queued jobs invoke the same shared application services rather than owning or duplicating business logic:

```text
FastAPI routers ─┐
                 ├→ shared services → domain / adapters / persistence
RQ jobs ─────────┘
```

Routers own HTTP concerns only; worker jobs own queue-execution concerns only. OpenOrc workflow/business behavior belongs in shared services and domain code and must not depend directly on FastAPI or RQ. HTTP API contracts should model OpenOrc resources, commands, and queries rather than Vue-specific screens, components, or interaction flows, so the same control-plane semantics can support additional clients and protocol surfaces without duplicating workflow logic.

The headless control plane is the OpenOrc product boundary; the Vue application is one first-party client of it, not the owner of workflow semantics. FastAPI/OpenAPI provides the canonical v1 network command/query surface. Future OpenOrc CLI and language SDKs may consume that same API contract, and a future MCP surface may project selected OpenOrc resources and commands for agent clients. Those surfaces must reuse the same application-service authorization, authority boundaries, state transitions, and command/query semantics rather than becoming parallel workflow engines or privileged back doors. Phase 2 does not require implementation of an OpenOrc CLI, SDK, or MCP server, but its application/API boundaries must allow them to be added without refactoring domain/workflow logic.

Python control-plane operational telemetry uses OpenTelemetry for traces, application logs, and metrics, exported through OTLP so the core application remains observability-backend-neutral. Observability is contextual only: telemetry must not become workflow authority, durable audit truth, or a reason to widen database transaction scope. Detailed local rules belong in the relevant nested `AGENTS.md` files.

Public OpenOrc documentation is docs-as-code owned by `openorc/core`. Human-authored guides and concepts live as Markdown/MDX under `docs/`; HTTP API reference is derived from FastAPI's OpenAPI contract; OpenOrc agent-protocol reference is derived from or links to the canonical JSON schemas under `src/openorc/protocol/`. The documentation renderer/publishing platform is replaceable and is never a source of product, API, or protocol truth.

## 17.3 Persistence and background work

```text
Supabase / Postgres
Valkey / Redis-compatible queue
RQ
```

Workflow state lives in Postgres, not in RQ jobs.

OpenOrc application/workflow persistence uses ordinary Python Postgres access against Supabase Postgres rather than treating the Supabase Data API / `supabase-py` as the primary persistence abstraction. Supabase-specific SDK/API use remains appropriate for Auth and other Supabase-owned service capabilities. Browser clients never access OpenOrc application data through Supabase directly: browser application reads/writes and live updates go through the OpenOrc API, while the browser's direct Supabase relationship is authentication.

Long-lived API/worker processes use explicitly bounded, configurable Postgres connection pools. Connection mode is deployment configuration: the reference deployment may use direct persistent connections while smaller, IPv4-constrained, or more horizontally scaled deployments may use an appropriate Supabase/Supavisor pooler without changing domain semantics. Persistence code should avoid unnecessary dependence on database session-local features when ordinary transaction-scoped behavior is sufficient.

OpenOrc-owned application tables live in a dedicated `openorc` Postgres schema; Supabase-managed Auth remains in Supabase's `auth` schema. Internal OpenOrc domain records use UUID primary keys except where an explicit 1:1 infrastructure identity such as `Profile` deliberately reuses the corresponding Supabase Auth UUID. Stable external GitHub/runtime identifiers remain attributes with appropriate uniqueness constraints rather than replacing OpenOrc domain identity.

Workflow/state vocabularies are represented as typed Python enums/value objects and database text columns with explicit CHECK constraints rather than native Postgres ENUM types. Real-world instants use PostgreSQL `TIMESTAMPTZ`; Python uses timezone-aware datetimes only, normalized to UTC at application/service boundaries, and APIs serialize explicit ISO-8601 offsets/`Z`. Local timezone is presentation only.

Schema migrations under `supabase/migrations/` are append-only once merged/shared. A migration under review may be corrected in place; changing already-shared schema history requires a new migration. Indexing is driven by demonstrated workflow/query paths and required uniqueness constraints rather than speculative indexing of every field.

## 17.4 CI, deployment, telemetry

```text
GitHub Actions
DigitalOcean

Python control-plane operational telemetry
→ OpenTelemetry traces + application logs + metrics
→ OTLP vendor-neutral export

Vue product SPA observability and analytics
→ PostHog product analytics + frontend error tracking
→ privacy-constrained session replay / web performance where useful

TanStack Vue Query
→ canonical server-state/query/mutation lifecycle
→ centralized frontend API-failure observation
```

These surfaces remain deliberately distinct from durable `WorkflowEvent` audit history and from connected-runtime telemetry. `WorkflowEvent` records consequential OpenOrc workflow/audit facts in Postgres; OpenTelemetry describes the operation of OpenOrc's own Python control-plane code; runtime telemetry remains runtime-owned activity consumed through adapters. None is a substitute for another.

### 17.4.1 Python operational observability

The Python API/worker control plane uses OpenTelemetry as the canonical operational-observability mechanism. Traces, ordinary Python application logs, and metrics are correlated through OpenTelemetry and exported through OTLP. Core does not depend on a PostHog-, Grafana-, Honeycomb-, or other backend-specific telemetry SDK for Python operational observability; deployment chooses the OTLP collector/backend.

Manual instrumentation belongs at meaningful OpenOrc boundaries such as API requests, queued work, application-service operations, and external adapter operations rather than every helper, guard, repository function, or SQL statement. Safe stable identifiers such as Workspace, Task, Execution, Connection, workflow role, GitHub stable IDs/exact SHAs, and queue-job identity may be attached where relevant. Telemetry must never include raw credentials, bearer/auth headers, deployment secrets, Workspace guidance prose, prompt bodies, source/customer content, agent transcripts/private reasoning, or arbitrary request/response bodies.

Every OpenOrc API request has a safe opaque server-side `request_id`. The API attaches it to the active request span/log context and returns it to the caller in a response header. The `request_id` is an operator-facing bridge for browser/backend diagnostics only: it is never authentication/authorization context, workflow authority, an idempotency key, or a substitute for the OpenTelemetry trace ID, and it is not threaded through domain/service method signatures as business data.

### 17.4.2 Vue SPA observability and product analytics

The v1 Vue SPA uses PostHog as its browser-side observability and product-analytics surface rather than requiring browser OpenTelemetry. PostHog may provide explicit semantic product events, frontend error tracking, privacy-constrained session replay, and web/performance analytics. Product analytics are observational only and never authorize or prove workflow actions; consequential success events are recorded only after the authoritative OpenOrc API operation succeeds.

TanStack Vue Query remains the canonical owner of server-derived state and the centralized seam for query/mutation failure observation. Shared query/mutation handling classifies ordinary expected product outcomes separately from operational failures worth reporting; components must not independently duplicate toast, log, and error-capture behavior for the same failure. The existing centralized transient API-error/toast path remains user-facing UX, not workflow truth or a second logging system.

PostHog identity should use canonical OpenOrc identity such as `Profile.id`, with Workspace context where useful, rather than mutable GitHub usernames/emails as authority. Browser telemetry must not capture credentials, auth tokens, Workspace guidance, issue/PR/plan/review bodies, prompt/agent content, source code, arbitrary API payloads, or other customer development content. Session replay uses conservative masking/blocking appropriate to a software-development control plane. Production browser console output is not the telemetry pipeline.

Frontend operational evidence may include the backend `request_id` returned for an API call. This allows a PostHog error/session observation to be correlated with the corresponding Python OpenTelemetry request trace without making browser distributed tracing a v1 requirement. Core/self-hosted operation must remain functional when PostHog is unconfigured; PostHog availability never affects workflow correctness.

## 17.5 Repository structure and ownership

OpenOrc is intentionally split across two repositories with different ownership boundaries. The trees below are the **starting structural intent**, not a promise that every folder name is immutable. Implementation may refine names and leaf layout, but should preserve the architectural boundaries and dependency direction rather than beginning from a generic `frontend/` / `backend/` split and moving code later.

### `openorc/core`

`openorc/core` is the complete open-source product. A proposed starting shape is:

```text
openorc/core/
├── apps/
│   ├── app/                         # Vue/Vite product SPA
│   ├── api/                         # thin FastAPI process/bootstrap
│   └── worker/                      # thin RQ process/bootstrap
│
├── src/
│   └── openorc/
│       ├── domain/                  # entities, state transitions, invariants
│       ├── services/                # shared use cases / workflow operations
│       ├── protocol/                # runtime-neutral OpenOrc agent contracts
│       ├── api/
│       │   ├── routers/             # HTTP transport only
│       │   ├── schemas/
│       │   └── dependencies/
│       ├── workers/
│       │   └── jobs/                # queue transport only
│       ├── adapters/                # GitHub, Agent Runtime, etc.
│       └── persistence/             # durable storage implementation
│
├── packages/
│   └── cline-sdk-bridge/            # thin Node/TS @cline/sdk compatibility bridge
│
├── supabase/
│   └── migrations/                  # product-owned schema/migrations
├── tests/
├── docs/
├── AGENTS.md
└── README.md
```

The Vue product surface is called `app`; `web` is reserved for a distinct public/marketing website if OpenOrc ever grows one. API and worker are independently runnable process surfaces, but they share the same Python services/domain implementation. Business logic must not be duplicated into routers or jobs.

Supabase is the supported v1 persistence/authentication implementation and therefore belongs in core: schema/migrations, persistence integration, and Supabase Auth integration are product code. This does not require a generic persistence/auth provider framework in v1.

The Cline SDK bridge is supporting package code, not a fourth deployment surface. It must remain a disposable local child-process integration behind the Python Cline adapter/backend boundary and must not acquire OpenOrc workflow semantics or durable state.

### `openorc/cloud`

`openorc/cloud` is private and composes the core product with the concrete OpenOrc Cloud deployment, dogfood runtime infrastructure, and hosted-only commercial behavior. A proposed starting shape is:

```text
openorc/cloud/
├── src/
│   └── openorc_cloud/
│       └── billing/                 # hosted-only billing/entitlement behavior
│           └── polar/               # Polar integration
│
├── deployment/
│   ├── AGENTS.md
│   ├── README.md
│   ├── do-app.yaml                  # one DO App Platform app:
│   │                                #   Vue SPA + FastAPI service + RQ worker
│   └── cline/                       # dogfood/reference runtime provisioning
│       ├── runtime-host/                 # disposable managed host primitive
│       └── cline-box/                    # accepted managed Cline Hub/dashboard topology
│
├── .github/
│   ├── AGENTS.md
│   └── workflows/                   # CI/deploy automation
│
├── operations/                      # Cloud runbooks/recovery/monitoring as needed
├── docs/
├── AGENTS.md
└── README.md
```

The Cloud repository depends on core; core must never depend on Cloud. Polar-specific code belongs only in Cloud behind the smallest explicit hosted-account/billing seam required by the core application. Self-hosted OpenOrc remains complete without that extension.

The Cloud `deployment/` directory owns the concrete DigitalOcean deployment model. The intended control-plane deployment is **one DigitalOcean App Platform app** containing the static Vue SPA, FastAPI service component, and RQ worker component. Managed Cline Boxes are separate runtime deployments outside that App Platform control plane; the accepted v1 baseline may host Producer and Reviewer as distinct Task-scoped sessions on the same persistent managed Cline Hub/Box with isolated working contexts. Exact runtime provisioning files may evolve as the deployment mechanism is implemented.

Production configuration values are owned individually by the GitHub `production` Environment as variables/secrets. The tracked DigitalOcean YAML owns desired-state structure. OpenOrc Cloud should not inherit Babelbeez's `.env.production.*` mirror blobs, custom `render_do_specs.py` configuration map, or persistent `generated/` deployment-spec convention. If a transient deploy-ready spec requires substitution, the workflow should perform generic one-shot materialization in runner-temporary storage rather than introducing a second configuration model.

The same boundary applies to other hosted infrastructure: core owns the Supabase schema and application integration; Cloud owns the concrete production Supabase project/operations. Core owns Redis/RQ behavior; Cloud owns the concrete managed Valkey connection/deployment configuration. Core owns the OpenTelemetry instrumentation/OTLP export contract and the optional Vue/PostHog browser integration; Cloud owns the concrete hosted OTLP collector/backend, PostHog project/configuration, telemetry retention/routing, and related operational runbooks. Core owns the Cline adapter/SDK bridge; Cloud owns the dogfood/reference Cline runtime infrastructure.

Repository-local `AGENTS.md` files should reinforce these boundaries. In particular, API routers remain transport-only, worker jobs remain queue-execution-only, and shared business/workflow behavior belongs in `src/openorc/services/` and `src/openorc/domain/`. Deployment and GitHub Actions rules belong near `deployment/` and `.github/` rather than bloating root guidance.

---

# 18. Reference Deployment and Runtime Operations

The reference deployment is intentionally practical and small. These choices are not universal product dependencies.

## 18.1 OpenOrc control plane

One DigitalOcean App Platform application contains:

```text
DigitalOcean App Platform
├── static Vue frontend
├── FastAPI web service
└── RQ worker
```

Initial scale:

```text
1 API service instance
1 worker service instance
```

The single worker service may run enough local RQ worker processes to avoid serializing independent Task dispatches when the configured agent runtimes have concurrent session capacity. v1 does not require horizontal worker autoscaling or a worker fleet.

The API and worker do not rely on local persistent filesystem state. The control-plane build/runtime environment includes Node 24 as part of the standard OpenOrc stack. Any API/worker process that requires Cline SDK access may run its thin SDK bridge as a local child process; this does not add another DigitalOcean App Platform service, port, network authentication boundary, or independently scaled deployment component.

## 18.2 Supabase

OpenOrc uses a dedicated Supabase project inside the Babelbeez Supabase Pro organization:

```text
Babelbeez Supabase organization
├── Babelbeez project
└── OpenOrc project
```

OpenOrc owns its own Postgres data and Auth boundary. The product schema/migrations and Supabase application integration live in `openorc/core`; the concrete production Supabase project, credentials, backups, monitoring, and other hosted operational concerns belong to `openorc/cloud`.

## 18.3 Valkey

The MVP reference deployment reuses the existing managed Valkey service:

```text
DB 0 = Babelbeez
DB 1 = n8n
DB 2 = OpenOrc
```

OpenOrc also uses OpenOrc-specific queue names/prefixes.

This is a deployment optimization, not a structural dependency. OpenOrc depends only on Redis-compatible connection configuration so a dedicated service can be introduced later without product-model changes.

## 18.4 Cline runtime

OpenOrc supports two operational models for its first Agent Runtime without changing Core workflow semantics.

**Externally managed / self-hosted runtime:** the Owner supplies and operates the Cline runtime Connection. Runtime deployment, upgrades, filesystem, provider/MCP/tool/repository credentials, local dependencies, and operational recovery remain the Owner's responsibility. Self-hosted OpenOrc is complete without OpenOrc Cloud runtime provisioning.

**OpenOrc Cloud-managed runtime:** Cloud may provision and operate a persistent development environment for a Workspace as an optional hosted capability. In v1 this environment is the ClineBox: the Workspace's long-lived cloud development machine running Cline as its Agent Runtime, not a disposable per-Task sandbox. It may contain multiple repositories belonging to the Workspace together with their dependencies/toolchains, caches, CLIs, runtime-native configuration, and other useful development state. Managed compute remains outside the persistent OpenOrc App Platform control plane and outside the API/worker filesystem. The normal Cloud lifecycle is Workspace-scoped and hibernatable: the environment is hydrated once, reused across Tasks, may host multiple concurrent isolated Task contexts when configured capacity permits, and is suspended by snapshotting only when no nonterminal Task depends on it. Clean reproducible bootstrap remains the replacement/recovery path rather than the ordinary start-of-work path.

```text
OpenOrc Cloud control plane (persistent)
        │
        ├── externally managed/BYO Cline runtime Connection
        │
        └── OpenOrc-managed Workspace development environment / ClineBox
              initial provision/bootstrap/hydration
                         ↓
                       READY
                         ↓
                 reused across Tasks
                         ↓
          no nonterminal Task dependencies
                         ↓
                    HIBERNATE
          quiesce → snapshot → destroy compute
                         ↓
                    HIBERNATED
                         ↓
                      RESTORE
          create compute from snapshot
          → readiness/reconciliation
                         ↓
                       READY

exceptional recovery/replacement
→ clean provision/bootstrap/hydration from reproducible definition + secure prerequisites
```

Normal hibernation or destruction of an OpenOrc-managed runtime is blocked while any nonterminal Task depends on sessions on that runtime. Hibernation is a Workspace-environment lifecycle operation, not a Task-session persistence mechanism, and is allowed only after those Task dependencies are gone. An explicit emergency override may destroy the runtime despite active dependencies; doing so deliberately loses those runtime sessions and causes the affected Task attempts to fail closed rather than silently replacing their sessions. Continuation uses cancellation/archive plus a fresh Task with fresh role sessions.

The retained hibernation snapshot may preserve the hydrated Workspace environment, including repository clones, installed dependencies/toolchains, caches, and other runtime-local filesystem state useful for fast resume. That does not make snapshot contents canonical OpenOrc workflow state or authority, and OpenOrc does not rely on hibernation to preserve active Cline conversational/session continuity.

Managed-runtime infrastructure must remain reproducible independently of any one running instance or retained snapshot. For the managed Cline Box, provider/MCP/plugin/tool/sub-agent configuration and credentials remain Cline-owned rather than becoming OpenOrc Cloud secrets simply because Cloud owns the host. A clean rebuild may require the Owner to re-establish that native Cline state through supported Cline surfaces. Managed-Box Git instead reuses Core's existing Profile-scoped Owner GitHub App user authorization; Cloud owns only the downstream materialization/refresh/revocation/rebuild mechanics needed to make that authorization usable from the Box, and Core does not gain Cline-Box-specific credential machinery.

The accepted managed-runtime baseline is the managed Cline Box from `openorc/cloud#16`: one persistent Cline Hub/dashboard with OpenOrc as a public `ClineCore` remote client. The remaining Cloud #2 work qualifies the documented session/control/configuration/reconciliation surfaces against the pinned release before Core D1 freezes the supported contract.

Multiple configured runtimes per role, generic runtime pools, scheduling/failover, and dynamic membership remain post-v1. The concrete provision/hibernate/restore/rebuild lifecycle of one configured OpenOrc Cloud-managed Workspace runtime is not a generic runtime-pool framework.

## 18.5 Network boundaries

Conceptually:

```text
First-party Vue client → OpenOrc API (HTTP commands/reads + SSE live events)
Future CLI / SDK clients → same OpenOrc HTTP/OpenAPI command/query semantics
Future MCP surface → selected OpenOrc resources/commands through the same application authority
Future webhook delivery → OpenOrc event notifications to configured external consumers
Browser → Supabase Auth
Browser → PostHog endpoint when browser observability/analytics is configured
OpenOrc API / Worker → Supabase
OpenOrc API / Worker → Valkey
OpenOrc API / Worker → OTLP collector/backend when operational telemetry export is configured
OpenOrc API / Worker → local Cline SDK bridge (stdio) → official Cline SDK → supported remote-runtime surface
```

The Vue application is one client of the headless OpenOrc control plane. It does not define the API shape or own workflow behavior. Browser application reads/writes and live events use the same OpenOrc command/query/event boundaries that other authorized clients may consume through their appropriate transports. Future CLI, SDK, MCP, or outbound-webhook surfaces must not introduce alternate workflow authority or bypass the shared application services.

The browser does not use Supabase as an application-data or realtime transport; its direct Supabase relationship is authentication. Browser-to-PostHog traffic, when configured, is observational only and carries no workflow commands or authority. API/worker OTLP export is likewise observational and must not affect workflow correctness. The API/worker-to-bridge hop is local process IPC over stdin/stdout; the bridge uses public `ClineCore` remote mode to the already-running managed Cline Hub/Box. Cloud owns how that endpoint is reached securely (for example the current loopback/SSH research transport); Core depends only on the public remote-session contract and not on a helper Hub or private wire protocol.

The Vue frontend is not served by FastAPI merely to collapse deployment. Frontend receives its API base URL through configuration. API and worker should run in the same DigitalOcean region as the shared Valkey service where practical.

## 18.6 Runtime configuration and rebuildability

OpenOrc's runtime surface distinguishes external-runtime configuration from managed-runtime reproduction and hibernation.

For an externally managed/BYO Cline runtime, OpenOrc remains primarily a Connection and role-binding surface. The runtime owner configures provider authentication, MCP/tools, plugins, skills, sub-agents/teams, Git credentials, dependencies, upgrades, and runtime-global settings through supported Cline surfaces. OpenOrc selects only the role/session configuration it needs, notably provider/model identifiers and the Task working context, and may display supported status/telemetry or link to native runtime management where useful.

For an OpenOrc Cloud-managed Cline Box, Cloud owns the reproducible host/runtime definition and OpenOrc control connectivity, while Cline remains canonical owner of Cline-native configuration and credentials. Managed-Box Git authorization is not Cline-native state: Cloud consumes Core's Profile-scoped Owner GitHub App authorization and is responsible for the eventual hosted delivery/refresh/revocation mechanics without making Core depend on Cline Box provisioning. A Workspace-scoped hibernation snapshot is an opaque infrastructure artifact that may preserve the already-hydrated development environment, but it does not turn native Cline settings or credentials into OpenOrc domain state. Clean rebuild may therefore require supported Owner reconfiguration of Cline-native state and, once implemented, fresh Cloud materialization of the Owner Git authorization.

The snapshot may therefore preserve runtime-private working state, caches, and filesystem contents that are useful to resume the Workspace environment. Those contents do not become OpenOrc workflow/audit state or repository authority merely because Cloud retains the snapshot. Hibernation is blocked while nonterminal Tasks depend on runtime sessions, so snapshot/restore is not used to preserve active Task-session continuity.

Conceptually:

```text
Workspace
├── externally managed runtime Connection
│   └── runtime owner manages runtime credentials/configuration
│
└── OpenOrc Cloud-managed runtime
    ├── reproducible runtime definition
    ├── OpenOrc-owned infrastructure/control configuration
    ├── Cline-owned native configuration/credentials
    └── hydrated Workspace environment
        ├── running compute instance while READY/active
        └── retained hibernation snapshot while compute is absent
```

Completed R2a.2–R4b research establishes the supported remote construction, working-context, interruption/reconciliation and loss contract summarized in §7.5 and §15.4. Role prompts are Core configuration, not Cloud hydration. Production managed Git token delivery remains downstream Cloud work using existing Core Profile authorization, not a prerequisite to implementing ClineAdapter. No generic credential entity or private Cline file becomes a public Core API.

The managed runtime must remain cleanly rebuildable from repository-owned Cloud desired state plus documented secure prerequisites even though normal daily resume uses the retained Workspace snapshot. Changes to Cloud-owned runtime definitions/configuration are tracked rather than depending on undocumented pet-VM mutation. Workspace development hydration may evolve inside the runtime and be preserved by hibernation, but loss or deliberate invalidation of the snapshot must fall back to a clean provision/bootstrap/hydration path rather than making that snapshot an irreplaceable source of truth.

## 18.7 Repository boundaries

`babelbeez/Babelbeez` owns the Babelbeez application, product tests, repository `AGENTS.md` hierarchy, branches, and product PRs.

`openorc/core` owns the complete open-source product: Vue application, API/worker process surfaces, workflow/domain services, GitHub integration, Reviewer coordination, runtime adapters, Cline SDK bridge, Supabase schema/auth integration, configuration UI, product tests/documentation, and `AGENTS.md` hierarchy. Core must remain runnable without the private Cloud repository.

`openorc/cloud` depends on core and owns hosted/reference concerns: the concrete DigitalOcean control-plane deployment, production Supabase/Valkey configuration and operations, managed/reference Cline runtime research/provisioning/lifecycle, OpenOrc-owned infrastructure/control credentials for that lifecycle, hosted-only Polar billing/entitlement behavior, Cloud runbooks, and Cloud deployment automation. Cline-native provider/MCP/plugin/tool credentials remain runtime-owned. For managed-Box Git, Cloud consumes Core's existing Profile-scoped Owner GitHub authorization and owns the hosted token-delivery/refresh/revocation/rebuild mechanics; Core exposes no Cline-Box-specific Git credential abstraction and must never depend on Cloud.

The reference Cloud control plane is one DigitalOcean App Platform application containing the Vue static-site component, FastAPI service component, and RQ worker component. Managed Cline Workspace environments remain separate from that persistent control-plane application and may provision, hibernate, restore, or rebuild their compute independently, subject to the nonterminal-Task hibernation guard. The accepted managed baseline uses one persistent managed Cline Hub/Box for both roles; R3 qualified separate per-role checkout working contexts with per-session cwd/workspaceRoot; this is not cross-Workspace security isolation. Separate role runtimes remain an optional hardened deployment choice, not the default contract.

Self-hosted users may supply and operate any conforming supported runtime deployment themselves. OpenOrc Cloud may additionally operate persistent managed Workspace development environments/ClineBoxes for hosted customers. This hosted capability does not move runtime execution into the shared control-plane process or make Cloud a dependency of self-hosted Core.

---

# 19. UI Boundary

The OpenOrc UI is a focused control plane, not an IDE and not a replacement frontend for GitHub, Cline Hub, or other connected systems.

The governing UX principle is:

> OpenOrc provides the workflow control surface and aggregates external state where orchestration requires it. It links to authoritative external systems for deeper inspection or management rather than reproducing native interfaces they already provide well.

The main dashboard is the primary execution surface. An Owner should be able to traverse the normal issue-to-merge workflow without leaving that view.

## 19.1 Dashboard Task wizard

The dashboard centers on a contextual Task wizard. Its user-facing phases describe the engineering flow rather than mirroring the internal state enum one-for-one:

```text
Pick Issue
↓
Plan
↓
Authorize Implementation
↓
Implement
↓
Authorize PR
↓
Review
↓
Merge
↓
Done
```

These are UX phases, not new domain states. Internal states such as repeated `WAITING_FOR_OWNER` occurrences remain workflow/domain concerns and are projected into the appropriate wizard context rather than exposed as duplicate user-facing steps.

Each active wizard occupies its own dashboard tab. A new wizard begins at issue selection and, once started, remains bound to that Task through its lifecycle. Multiple tabs allow the Owner to keep several concurrent Tasks visible without introducing a global `current_task` assumption in the frontend.

The current wizard context surfaces only the information and controls needed for the current phase. Internal branch/worktree plumbing is not a user-facing workflow phase; the Owner normally continues to reason about the work as the same Task throughout. Examples include:

- issue identity and eligibility before Task start;
- current PlanRevision and Reviewer result/findings during planning;
- review-cleared plan at implementation authorization;
- Producer activity and implementation result during implementation;
- implementation result and Owner validation decision at PR authorization;
- PR identity/head, Reviewer state/findings, and GitHub CI/check state during review;
- current reviewed PR head plus GitHub CI/check state at merge decision.

Issue selection should display authoritative GitHub hierarchy for orientation and dependency state for sequencing. Parent/sub-issue shape does not affect executability; an issue GitHub currently reports as blocked cannot start until GitHub no longer reports it blocked. The UI should explain dependency-blocked or otherwise ineligible states rather than allowing an invalid start attempt to become a generic backend error. If a started Task is waiting for runtime capacity, the Plan context may show it as `QUEUED` and waiting its FIFO turn without presenting the condition as an error or Owner intervention gate.

Task progress must not imply a percentage, ETA, or completion fraction when OpenOrc has no authoritative denominator. The wizard exposes explicit workflow phase and current facts instead of synthetic progress bars.

## 19.2 Contextual interruptions

Exceptional conditions do not become additional happy-path wizard phases. They interrupt the current Task context while preserving the underlying workflow phase.

Examples include:

```text
optional adapter-mediated RuntimeRequest requires Owner input
REVIEW_RESOLUTION after ReviewLoop limit exhaustion
BLOCKED / recovery required
agent session loss
Connection/runtime failure
```

The current Task tab must make the required Owner action or recovery condition prominent. Cross-Task queues for these conditions live in dedicated control surfaces such as Owner Requests or OpenOrc Tasks.

At Reviewer-related owner gates, `Discuss` routes to the existing Reviewer session and never directly changes workflow state.

At `REVIEW_RESOLUTION`:

```text
[Discuss] [Approve] [Cancel]
```

`Approve` records an explicit Owner override of the unresolved Reviewer objection for the exact exhausted review subject. It does not rewrite the Reviewer result or restart the Producer ↔ Reviewer loop.

OpenOrc does not expose a general Owner ↔ Producer chat surface.

## 19.3 Minimal Task-scoped agent telemetry

Minimal agent health/activity is a persistent companion to each Task wizard tab and remains separate from Task workflow state.

The compact traffic-light model is:

```text
GREY    UNKNOWN       runtime telemetry unavailable, unsupported, or not currently observable
RED     UNAVAILABLE   Task role session is known unhealthy or lost
YELLOW  CONNECTING    connecting, creating, or initializing the Task role session
GREEN   READY         initialized, healthy, and idle/waiting
BLUE    WORKING       runtime reports active work
```

Color is always accompanied by a textual label. `UNKNOWN` does not mean unhealthy.

The traffic light is scoped to the Task's Producer or Reviewer session. Runtime-level capacity is a separate Workspace/configuration concern. Where a runtime exposes capacity, the UI may show usage such as available/used session slots; lack of a free slot is not itself equivalent to a red/unhealthy Task session.

OpenOrc consumes supported runtime telemetry into its own workflow-facing UX. A dedicated Agent Telemetry surface may expose richer runtime activity such as content updates, tool activity, usage/cost, notices, completion/failure signals, session health, and capacity where the adapter supports them. Where the adapter exposes supported runtime activity controls, the Task-scoped UI must also let the Owner cancel/stop and resume the exact Producer or Reviewer session activity when those actions are currently applicable. These controls are visually and semantically distinct from **Cancel Task**: runtime cancel/resume preserves the OpenOrc Task and the same bound TaskAgentSession; Cancel Task ends and archives the Task attempt. OpenOrc should not grow into a full runtime transcript or IDE replacement merely because one runtime exposes a rich event stream.

## 19.4 Dedicated control surfaces

The sidebar provides dedicated surfaces for inspection, administration, recovery, and cross-Task attention rather than replacing the dashboard wizard as the normal execution flow.

Canonical v1 navigation is conceptually:

```text
GitHub Issues
OpenOrc Tasks
Agent Telemetry
Owner Requests
Activity
Configuration
Account Settings
```

Responsibilities are:

```text
GitHub Issues
→ synchronized issue backlog, hierarchy/dependencies, eligibility, Start Task

OpenOrc Tasks
→ complete Task records across active/completed/cancelled/blocked work

Agent Telemetry
→ richer runtime/session activity and health where adapters expose it

Owner Requests
→ cross-Task Owner attention queue for lifecycle gates and scoped Producer/runtime action-approval RuntimeRequests

Activity
→ durable OpenOrc workflow/audit history, updated live through SSE

Configuration
→ Connections, role assignments, runtime configuration/status, Workspace guidance, Workspace policy

Account Settings
→ account-level settings and hosted billing/account concerns where applicable
```

`Activity` represents durable OpenOrc workflow/audit history rather than promising storage of every high-frequency runtime event. Runtime-native logs/transcripts remain in the runtime's own tooling where practical.

## 19.5 External native surfaces

External systems remain authoritative for the interfaces they own well.

Conceptually:

```text
GitHub-owned concern
→ summarize/control in OpenOrc where orchestration requires it
→ link to GitHub for authoritative deep inspection/management

runtime/provider-owned concern
→ summarize/control in OpenOrc where orchestration requires it
→ link to the native runtime/provider UI where available

OpenOrc-owned concern
→ handled fully inside OpenOrc
```

For the v1 Agent Runtime, the Cline native management/session UI remains the owner surface for provider authentication, MCP, plugins, tools, skills, sub-agents/teams, permission configuration, and other Cline-native behavior. OpenOrc Cloud-managed Boxes do not duplicate those settings merely because Cloud owns the host lifecycle. `ClineAdapter` may expose supported external management or session-specific links without making those native surfaces part of OpenOrc workflow authority.

External links are affordances, not integration transports. OpenOrc application reads/writes, workflow-changing actions, and runtime telemetry remain mediated by the OpenOrc API and adapters. Following an external link is a deliberate user navigation into that external system's own authority and security boundary.

## 19.6 Error presentation and durable failure state

Ordinary transient backend/API failures surface through centralized frontend error notifications/toasts.

A transient notification does not replace durable workflow state. If a failure produces a persistent domain consequence such as `BLOCKED`, `AGENT_SESSION_LOST`, or `PR_CLOSED_UNMERGED`, that condition remains visible whenever the Task is reopened until the workflow is resolved.

## 19.7 Configuration and interaction-guidance boundary

The Configuration surface may expose Workspace-owned Connections, role assignments, supported runtime configuration/status, Workspace guidance, and Workspace policy. For an OpenOrc Cloud-managed runtime, this surface may also expose hosted lifecycle status and actions such as `PROVISIONING`, `READY`, `HIBERNATING`, `HIBERNATED`, `RESTORING`, and `FAILED`. These are Cloud-managed runtime lifecycle/UX states, not Core Task states, `TaskAgentSession` states, or additions to the universal Agent Runtime contract. **Hibernate** is unavailable while any nonterminal Task depends on the runtime; **Resume** restores the retained Workspace snapshot and must complete readiness/reconciliation before the runtime is presented as `READY`. A deliberate clean rebuild remains the recovery path when the snapshot is unavailable or invalid.

For Cline role bindings, OpenOrc accepts opaque provider/model IDs and an optional Owner role-prompt Markdown override; NULL selects the shipped role default. Model ID is available from native Cline UI; provider ID is distinct and not assumed discoverable there. No catalog, configured-provider enumeration or effective readback is required. Native authentication stays Cline-owned; unusable selection fails during the first-send initialization before READY. Reviewer stays PLAN; Producer uses PLAN for planning and authorized ACT for implementation/remediation through the same-ID rebuild.

The Workspace Guidance experience is a simple optional Owner-authored prose setting, blank by default. OpenOrc controls where that guidance is inserted. Formal schemas, initialization prompts, workflow controls, protocol envelopes, session semantics, authority rules, and state transitions are not editable.

Raw secrets are never returned through ordinary APIs or rendered back into the UI.

## 19.8 Mobile and PWA posture

The OpenOrc frontend should be responsive and mobile-friendly for the core Owner workflow, especially Task monitoring, Owner requests, Reviewer discussion, Task-tab switching, and merge decisions. Dense inspection and configuration surfaces may make fuller use of desktop space without making mobile an afterthought.

The frontend should also be implemented with Progressive Web App (PWA) use in mind so OpenOrc can provide an installable app-like experience where supported. Exact manifest, service-worker, caching, installation, and responsive-layout mechanics remain implementation-level decisions.

PWA support does not imply offline workflow execution or a second client-side source of truth. OpenOrc workflow authority remains server-side; a disconnected client should not invent or advance state.

---

# 20. Testing Strategy

Testing should be behavior-focused, deterministic, and independent of live infrastructure for ordinary unit/integration runs.

Initial layers include:

```text
frontend component/state tests
API contract tests
domain/service tests
workflow transition tests
repository/persistence tests
RQ job tests
adapter contract tests
formal response protocol tests
Reviewer contract tests
agent-session lifecycle/isolation tests
schema/init/workflow-control/Workspace-guidance tests
communication-topology tests
GitHub adapter tests
```

The workflow state machine must be heavily testable without live Supabase, Valkey, Cline Hub, or model inference.

Session and adapter tests must prove:

- each new executable Task creates fresh isolated role sessions;
- initialization happens before workflow work;
- invalid/missing `session_ready` fails creation;
- repeated session-creation jobs do not replace initialized bindings;
- stage/retry transitions preserve existing bindings;
- cross-Task session IDs are never reused;
- multiple Tasks may hold and use independent role sessions concurrently when configured runtime capacity permits;
- runtime capacity exhaustion moves started Tasks to `QUEUED` rather than `BLOCKED`, and queued Tasks advance FIFO by queue-entry time when required capacity becomes available;
- runtime capacity exhaustion does not cause session reuse or cross-Task contamination;
- concurrent Producer Tasks have isolated mutable working contexts and never share/write the same feature branch;
- Producer and Reviewer sessions on the same Hub receive separate working contexts; Reviewer stays in Cline PLAN mode and formal review is prepared against the exact committed subject;
- role-specific initialization is delivered without replacing Cline's native system/agent harness;
- Python `ClineAdapter` contract tests can run against a fake `ClineSdkBackend` without starting Node or a real Cline Hub;
- the Node/TypeScript SDK bridge preserves request/reply correlation and asynchronous event forwarding over its stdio protocol, owns no durable OpenOrc state, and can be restarted without implying replacement of an external Task session;
- the supported Cline CLI + `@cline/sdk` semantic-version pair passes the real adapter/integration contract suite before either pin is adopted or upgraded;
- Cline v1 persists opaque per-role provider/model selections and optional Owner role-prompt overrides; runtime-reported provenance remains optional and requested identity is not mislabeled effective readback; provider credentials stay Cline-owned;
- shared-Hub capacity accounting counts all Producer and Reviewer sessions against the underlying runtime Connection rather than treating role bindings as separate pools;
- initial implementation completion reports a Producer-created branch as a routing/discovery claim; after push, GitHub reconciliation confirms the canonical Task branch and committed head, and runtime-local/sandbox Git state cannot substitute for that authority;
- missing/lost sessions do not trigger silent replacement;
- adapter errors reach deterministic recovery policy;
- Owner discussion can reach only the Reviewer session;
- formal responses conform to the adopted schemas;
- malformed/schema-invalid formal responses are rejected rather than semantically inferred;
- PR composition returns validated `pr_result` without allowing the Producer to perform PR workflow operations;
- canonical PR publication preserves a valid Producer-authored Task closing reference or appends one deterministically when absent;
- an acknowledged outbound dispatch is not resent after worker/job replay;
- a timeout/uncertain external-operation outcome blocks without automatic replay;
- known explicit failures receive only the bounded retry policy applicable to that operation and then block;
- stale delayed/replayed workflow operations cannot mutate a newer Task subject/gate and instead stop for Owner-driven recovery;
- delivery deduplication does not depend on LLM behavior;
- GitHub webhook delivery triggers deterministic authoritative-state reconciliation rather than becoming a competing source of GitHub truth;
- webhook signature validation and delivery-identity deduplication happen before workflow effects are applied;
- failed/missed GitHub webhook delivery can be recovered by the integration's reconciliation path without relying on GitHub automatic redelivery;
- replayed, missed, or out-of-order GitHub notifications do not corrupt Task state;
- SSE clients receive authorized OpenOrc live events without making SSE itself the durable source of workflow truth;
- optional runtime telemetry can drive `UNKNOWN` / `UNAVAILABLE` / `CONNECTING` / `READY` / `WORKING` display states without requiring telemetry support from every adapter;
- high-frequency runtime telemetry is not automatically persisted as durable workflow history;
- optional external runtime/provider links remain navigation affordances rather than OpenOrc application-data or control transports.

Review tests must prove:

- the default five-evaluation ReviewLoop limit;
- Workspace configurability;
- `REVIEW_RESOLUTION` gate creation;
- enabled Owner `Approve` override at that gate;
- `Discuss`, `Approve`, and `Cancel` behavior;
- Owner override preserves the final Reviewer `CHANGES_REQUESTED` rather than fabricating `ACCEPTED`;
- exhausted review subjects do not resume automatic Producer ↔ Reviewer iteration after Owner override;
- discussion prose cannot change workflow state;
- a changed PR head invalidates prior acceptance and causes the new head to require review;
- target/base-branch movement alone does not invalidate Reviewer acceptance when the PR head is unchanged;
- `PR_AUTHORIZATION` is bound to the exact committed head presented to the Owner and becomes stale if that head changes before PR publication;
- if the Task branch changes in the PR-creation race window after preflight but before/post creation reconciliation, the created PR is recorded, Reviewer dispatch does not occur, and the Task stops as a stale operation for Owner recovery;
- merge requests carry the exact reviewed/Owner-overridden head SHA as the expected GitHub merge SHA so a concurrently changed head cannot satisfy a stale merge decision;
- GitHub CI projection can account for both Checks and commit statuses on the exact current head without becoming an OpenOrc merge guard;
- repository-base movement after plan acceptance does not automatically invalidate the PlanRevision.

If the validated v1 runtime contract requires adapter-mediated `RuntimeRequest`s, tests must prove they are captured, correlated to the exact Task/session/request, surfaced to the Owner, answered through typed scoped controls, and resumed in the same bound session without creating a free-form Owner ↔ Producer chat path or general Owner-decision channel. Do not require such tests for Cline-native approvals/questions that never cross the OpenOrc adapter boundary.

Runtime-control tests must prove that an Owner-triggered supported runtime cancel/stop does not cancel the OpenOrc Task, mark the TaskAgentSession lost/ended, replace its external session identity, or create a free-form Owner ↔ agent message; supported resume continues through that same bound external session/context. The concrete adapter contract must distinguish runtime activity interruption/resumption from terminal OpenOrc Task cancellation, and tests must cover whichever Execution transition/history semantics the runtime's supported API actually provides.

GitHub source-change tests must prove that an authoritative issue-requirement change after Task start blocks the current Task, that OpenOrc does not semantically merge the new requirements into the existing attempt, and that a fresh Task for the same issue is permitted only after the old attempt is cancelled/archived.

Cancellation tests must prove that OpenOrc requests runtime abort for active work, ends/archives the Task and releases the GitHub issue even if abort outcome is uncertain, and never implicitly mutates GitHub issues, branches, commits, PRs, or CI/check state.

---

# 21. Open Source, Cloud, and Billing

## 21.1 Open source

OpenOrc core is intended for Apache License 2.0.

Self-hosting is a complete product, not a crippled community edition. Official runtime adapters, Connection contracts, and core workflow/domain semantics remain usable without OpenOrc Cloud.

The OpenOrc / Open Orchestrator name may remain protected through trademark policy independently of the software license.

## 21.2 OpenOrc Cloud

OpenOrc Cloud is the official managed deployment of the same core application:

```text
                  OpenOrc
                     │
          same core application
             ┌───────┴────────┐
             │                │
        Self-hosted      OpenOrc Cloud
        user operates    OpenOrc operates
```

The commercial distinction includes operational responsibility for the OpenOrc control plane: hosting, upgrades, persistence, backups, Supabase/GitHub sign-in configuration, GitHub App/webhook infrastructure, monitoring, recovery, and support. Hosted customers may either connect an externally managed/BYO Agent Runtime or use an optional OpenOrc Cloud-managed persistent Workspace development environment, implemented in v1 as the ClineBox running Cline as its Agent Runtime. That environment is separate from the shared control-plane process, is reused across the Workspace's repositories and Tasks, and may host multiple concurrent isolated Task contexts subject to configured capacity. Cloud provisions and operates its running compute, may hibernate it into a retained Workspace snapshot when eligible, and restores or cleanly rebuilds it as required.

Self-hosted users receive the complete Core product and remain responsible for their own Agent Runtime deployment/lifecycle. Hosted users should be able to move to self-hosting without changing the core workflow model or becoming structurally dependent on a bundled model provider. Provider/model usage remains between the runtime/customer and the chosen provider unless a separate future commercial decision explicitly changes that boundary.

## 21.3 Billing

OpenOrc Cloud uses Polar as Merchant of Record.

Billing is intentionally simple:

```text
one OpenOrc Cloud account
↓
one Polar billing customer/subscription
↓
quantity = active Workspace count
↓
charge = fixed Workspace unit price × quantity
```

There are no adopted seat-, token-, model-usage-, execution-volume-, or feature-tier billing dimensions for the base hosted control plane. Optional OpenOrc Cloud-managed Workspace development-environment compute is billed separately by running compute time (presented to customers as per-minute managed-runtime usage while compute is running). A hibernated runtime does not accrue running-compute time. Any included allowance or separate charging for retained snapshot/storage, plus exact runtime classes, rates, rounding/minimums, and infrastructure margin, is Cloud commercial configuration rather than Core workflow semantics.

There is no permanent free tier for OpenOrc Cloud. Users pay for active hosted Workspaces and, where used, managed runtime compute, or self-host the complete open-source product.

Active Workspaces are billable. Archived Workspaces retain history but are non-billable. Self-hosted deployments may operate any number of Workspaces.

Exact currency and unit price are commercial configuration rather than domain semantics.

Cloud billing may gate creation or activation of hosted Workspaces but must never weaken Workspace isolation or alter core workflow semantics. Polar-specific implementation belongs in `openorc/cloud`, behind the smallest explicit hosted-account/billing seam needed by core. Self-hosted core uses no Polar dependency and remains a complete product. The dependency direction is `cloud → core`, never `core → cloud`.

---

# 22. Future Engineering Signal Intake

Signal intake is post-v1 but part of the product direction.

Engineering work may originate from machine-readable observations such as:

- product analytics;
- error/performance monitoring;
- SEO tooling;
- dependency/security advisories;
- infrastructure monitoring;
- API/vendor change notifications;
- support/operational systems;
- other engineering signal sources.

Long-term intake flow:

```text
External signal
↓
Normalize
↓
Classify
↓
Correlate with existing signals/issues
↓
Determine engineering relevance
↓
create/enrich work when appropriate
↓
enter the normal OpenOrc engineering workflow
```

Possible classifications include:

```text
Confirmed defect
Potential defect
Security/dependency risk
Breaking-change risk
Technical hygiene
Product/UX friction
Informational vendor update
Irrelevant/marketing notification
```

Not every notification deserves an issue. Not every issue deserves immediate planning. Not every plan deserves implementation.

Critical invariants:

```text
automatic detection
≠
automatic authorization
```

```text
automatic issue creation
≠
automatic implementation approval
```

Signal-derived work enters the same review and human-authorization workflow as human-originated work.

Signal sources remain replaceable. OpenOrc consumes observations without becoming structurally dependent on one analytics, observability, SEO, security, dependency, email, or vendor platform.

> Automate observation, triage, and preparation aggressively while preserving explicit human authority at consequential boundaries.

---

# 23. Success Criteria

OpenOrc should optimize for outcomes rather than agent activity.

Useful outcomes include:

- fewer important engineering signals lost when signal intake exists;
- fewer duplicate issues;
- faster conversion of real problems into actionable work;
- technically sound plans;
- merged changes;
- fewer escaped defects;
- reduced human coordination and monitoring time;
- lower implementation cost;
- faster cycle time;
- fewer unsafe actions;
- successful recovery from failures;
- clear human ownership of consequential decisions.

OpenOrc should not optimize for token volume, session count, notification count, automatically created issue count, generated lines of code, commits per hour, or autonomous runtime duration.

The intended v1 experience is:

```text
engineering work defined
↓
GitHub issue exists
↓
optional hierarchy organizes related work
↓
GitHub dependency state permits work to start
↓
OpenOrc starts Producer planning
↓
Producer ↔ Reviewer iterate
↓
review-cleared plan published to GitHub
↓
Owner authorizes implementation
↓
Producer works remotely
↓
scoped action-approval RuntimeRequests surface only when needed
↓
Producer reports completion
↓
Owner performs Workspace-required validation
↓
Owner authorizes PR
↓
Producer composes PR title/body (`pr_result`)
↓
OpenOrc creates PR on behalf of Owner
↓
Producer ↔ Reviewer remediate committed state
↓
Reviewer accepts current head
↓
UI shows Reviewer state + GitHub CI/check state + Merge
↓
Owner requests merge
↓
GitHub confirms merge
↓
Task completed
```

The developer laptop is not required for long-running planning or implementation. Routine Producer ↔ Reviewer copy/paste disappears. GitHub remains the durable engineering record. Human authority remains explicit.

---

# 24. Implementation Strategy and Sequencing

OpenOrc implementation is optimized for the actual development model used to build it rather than for a conventional multi-human software team.

The working implementation loop is:

```text
Owner + ChatGPT
→ product/design decisions + architecture
→ Grounding Document

ChatGPT
→ converts settled architecture into durable repository guidance
→ decomposes implementation into GitHub issues/sub-issues/dependencies
→ reviews plans and PRs against architectural intent

GitHub
→ durable implementation task graph + shared engineering record

Cline
→ plans and implements one executable GitHub issue at a time
→ uses the live repository, tools, runtime, database, and infrastructure

Owner
→ temporarily relays messages between ChatGPT and Cline until OpenOrc itself replaces that coordination work
```

This division is intentional. ChatGPT has the broadest architectural context because it carries this grounding document. Cline has the broadest implementation context because it has the live repository and operational tool access. The implementation process should exploit that asymmetry rather than forcing either agent to reconstruct the other's context.

## 24.1 Knowledge handoff before implementation

Before substantive implementation begins, the grounding document is compiled into repository-native knowledge at three levels:

```text
README.md
→ what OpenOrc is, how the product is positioned, and how to get started

AGENTS.md hierarchy
→ durable architectural invariants and implementation constraints
→ root guidance for global rules
→ nested guidance for backend, frontend, persistence, adapters, deployment, and other domains

GitHub issues
→ specific implementation work
→ acceptance criteria
→ required tests
→ relevant architectural constraints
→ explicit dependencies/non-goals where ambiguity would be dangerous
```

The grounding document must not be copied wholesale into every issue. Durable cross-cutting knowledge belongs in `AGENTS.md`; issue-local work belongs in the issue.

GitHub Projects are optional views over this implementation graph rather than an architectural requirement. The durable planning structure is the issue hierarchy, issue dependencies, milestones/releases where useful, and the repository itself.

## 24.2 Issue decomposition model

Implementation planning should use GitHub-native hierarchy and dependencies:

```text
implementation area / epic parent issue
├── executable implementation issue A
├── executable implementation issue B
└── executable implementation issue C

issue dependency edges
→ determine implementation order where required
```

Parent issues may organize implementation work. Executable implementation issues are the units Cline plans, implements, tests, and submits for review.

A well-formed executable issue should normally define:

- the capability or behavior that must exist;
- the relevant architectural invariants it must preserve;
- required interfaces/contracts where already settled;
- dependencies on prior implementation work;
- acceptance criteria;
- required automated tests;
- explicit non-goals where scope could otherwise drift.

Issues should avoid unnecessary implementation micromanagement. Concrete file layout, helper structure, and internal implementation choices should normally be left to Cline's PLAN phase unless the repository architecture or an existing contract already makes them authoritative.

## 24.3 Implementation sequence

The implementation sequence is deliberately more horizontal at the beginning than a conventional vertical-slice methodology because OpenOrc's architecture has already been designed in unusual depth before implementation begins, and because Cline can validate large portions of the system headlessly through direct API, runtime, database, Git, and infrastructure access.

The intended sequence is:

### Phase 0 — Bootstrap and Repository Foundation

Establish the repository and engineering environment required for all later work.

Includes, as needed:

- repository creation and protection/configuration;
- Apache-2.0 license;
- `README.md`;
- root and nested `AGENTS.md` scaffolding;
- explicit `src/openorc/protocol/` package boundary for runtime-neutral OpenOrc agent contracts;
- `openorc/core` application/process shells (`apps/app`, `apps/api`, `apps/worker`) plus shared Python package boundaries;
- Python/Node package and quality tooling;
- test runners;
- CI baseline;
- Supabase project/bootstrap configuration;
- migration framework;
- Valkey/RQ connectivity;
- `openorc/cloud` deployment/repository skeleton, including the single App Platform control-plane spec boundary and reference Cline runtime deployment area;
- local/remote configuration conventions;
- secret/bootstrap boundaries.

Outcome:

```text
repository can be cloned
→ development environment can be established
→ app/API/worker/shared-service/test and Cloud-deployment skeletons run
→ implementation can proceed without repeatedly reinventing project infrastructure
```

This phase is intentionally horizontal. It exists to make the repository capable of being developed.

### Phase 1 — Domain and Persistence Foundation

Implement the durable control-plane model already defined by this document.

Includes the persistence and domain foundations required for later workflow behavior, such as:

- deliberate selection and establishment of the Python direct-Postgres persistence access layer and bounded pool configuration;
- OpenOrc `Profile` / Workspace / Project / Repository ownership model;
- Agent Runtime Connections and role bindings;
- Task and GitHub identity mapping;
- TaskAgentSession;
- PlanRevision;
- ReviewLoop / ReviewIteration;
- OwnerGate;
- Execution;
- RuntimeRequest;
- TaskBlock;
- TaskPullRequest;
- WorkflowEvent;
- current-Task uniqueness/archive semantics, including archived `CANCELLED`/`COMPLETED` attempts and fresh Tasks for reopened completed issues;
- Task `state_token` and exact-subject stale-operation protection;
- current-object pointer semantics without duplicated state;
- immutable historical evidence vs mutable operational/configuration records;
- direct Workspace scoping and consistency constraints;
- dedicated `openorc` schema, timestamp/enum/migration/index conventions, and explicit Owner-controlled purge boundaries;
- core persistence constraints and migrations;
- updates to relevant repository `AGENTS.md` files so durable persistence/domain invariants live near the implementation they constrain.

Domain and persistence tests are implemented with the schema rather than postponed until later integration. GitHub webhook intake/deduplication persistence and concrete external-operation dispatch/idempotency storage remain Phase 2 adapter/application concerns even though Phase 1 establishes the persistence primitives and stale-state invariants they rely on.

Outcome:

```text
OpenOrc's durable workflow/control truth can be represented and tested independently of live runtimes
```

This phase implements the domain model from the grounding document; it is not permission to invent speculative entities beyond demonstrated v1 requirements.

### Phase 2 — Backend Control Plane

Implement the headless OpenOrc product against the durable domain model.

This includes, incrementally and through independently reviewable implementation issues:

- GitHub-only sign-in through Supabase Auth, Profile bootstrap, and OpenOrc authorization boundary;
- first-class Workspace configuration for review limits and optional blank-by-default Workspace guidance;
- canonical OpenOrc JSON schemas for `session_ready`, `plan_result`, `review_result`, `implementation_result`, and `pr_result`, with explicit `schema_version` for machine compatibility;
- non-overridable Producer and Reviewer initialization prompts that render/reference the canonical schemas rather than duplicating them;
- non-overridable OpenOrc workflow controls whose concrete runtime realization may be prose, a runtime-native action, or both;
- controlled injection of current Workspace guidance where useful, without prompt-template keys, prompt/guidance versioning, hashes, snapshots, or per-interaction prompt-use audit history;
- GitHub App installation persistence/routing and authoritative-state reconciliation;
- Profile-scoped GitHub App user-to-server authorization, same-human identity binding, secure refresh-token lifecycle, and Owner-attributed engineering-record mutations;
- GitHub App webhook authentication/deduplication/reconciliation;
- GitHub hierarchy/dependency synchronization, hierarchy-only view/inform projection, and GitHub-authoritative dependency-blocking Task-intake guard;
- workflow/domain application services;
- Task state machine;
- internal language-neutral Cline SDK backend contract;
- thin Node/TypeScript `@cline/sdk` bridge as a long-lived local subprocess using JSON-RPC over stdin/stdout, with no workflow semantics or durable state;
- Python `ClineAdapter` built against that backend boundary rather than the Cline Hub wire protocol;
- consumption of the `openorc/cloud#2` empirical managed-runtime validation, grounding-document reconciliation, and selection/pinning of the supported current Cline release baseline before Cline implementation leaves are finalized;
- fake Agent Runtime adapters;
- Task-scoped Producer/Reviewer sessions;
- `session_ready`, `plan_result`, `review_result`, `implementation_result`, and `pr_result` validation;
- planning ReviewLoops;
- implementation authorization;
- Producer execution;
- runtime action approvals;
- cancellation/recovery semantics;
- canonical branch verification;
- PR authorization and publication;
- commit-addressed PR review/remediation;
- merge decision and GitHub merge invocation;
- RQ/background jobs;
- dispatch reconciliation/idempotency protections;
- SSE/live OpenOrc events over transport-independent OpenOrc event semantics;
- optional runtime telemetry boundary;
- complete client-independent HTTP/OpenAPI surfaces for OpenOrc resources, commands, and queries, suitable for the later Vue client and future CLI/SDK/MCP projections without duplicating workflow logic.

Phase 2 need not implement an OpenOrc CLI, language SDK, MCP server, or outbound webhook publisher. It must establish application, API, authorization, and event boundaries clean enough that those surfaces can be added later as clients/projections or event-delivery transports without changing core workflow/domain semantics.

Automated tests are written with each capability. Fake adapters are used aggressively so ordinary workflow tests do not depend on live inference or remote infrastructure.

The localhost-first implementation rule applies to the OpenOrc control plane, not to the Agent Runtime. Before Core Phase 2D D1 is made implementation-ready, `openorc/cloud#2` establishes the DigitalOcean runtime primitive, validates the Workspace-scoped bootstrap/hibernate/snapshot-restore/rebuild lifecycle, and empirically validates the current official Cline remote/SSH/runtime, credential/configuration, reconnect, and terminal-loss behavior against real hosts. Its R5 synthesis identifies validated facts, rejected older assumptions, exact tested releases/components, lifecycle timing/constraints, and required grounding-document changes. The grounding document is reconciled from that evidence before D1 selects/pins the supported Cline baseline and D2–D7 are refined/implemented. Core's Vue/API/worker control plane remains local during this research and later connects outward through the normal runtime adapter/Connection boundary. Ordinary automated tests continue to use fake runtime adapters; live managed-runtime checks are explicit qualification/integration validation. Unrelated Phase 2 work does not wait on Cloud #2.

Outcome:

```text
OpenOrc can execute the complete v1 workflow headlessly through a client-independent API/application surface; the later Vue application is one first-party client rather than the product boundary
```

The control-plane phase should be decomposed into many small GitHub issues. It is a sequencing phase, not one giant implementation ticket. In particular, the Cline integration should normally separate the SDK backend/bridge contract, the Python `ClineAdapter`, and real Cline CLI + `@cline/sdk` compatibility validation into dependency-linked implementation issues so the temporary language bridge remains independently testable and replaceable.

### Phase 3 — Real Headless End-to-End Validation

Before substantial frontend implementation, exercise the control plane against real infrastructure.

The integration environment should use the real relevant systems where practical:

```text
GitHub test repository
Supabase
Valkey / RQ
validated dogfood/reference Cline managed runtime environment(s) using the supported release baseline
OpenOrc control plane using the pinned supported Cline SDK/backend path through the local bridge
real Producer + Reviewer Task sessions
```

The goal is to prove the architecture rather than merely the mocks. Phase 3 consumes the real managed/reference Cline runtime primitive and supported baseline already established through Cloud #2 and Core Phase 2D; it is not the point at which the managed runtime contract is first discovered.

The headless validation should exercise the complete happy path as far as practical:

```text
GitHub issue
→ OpenOrc Task
→ Producer + Reviewer session initialization
→ Producer plan
→ Reviewer review / revision loop
→ Owner implementation authorization through API/control surface
→ Producer implementation
→ verified branch + committed head
→ PR authorization
→ Producer PR composition
→ OpenOrc PR creation
→ Reviewer PR review
→ Producer remediation where required
→ merge decision
→ GitHub-confirmed merge
→ COMPLETED
```

It should also exercise the highest-risk architectural boundaries discovered during design, including session continuity, exact-subject review, webhook reconciliation, runtime action approval, stale-operation protection, and cancellation/recovery behavior.

Outcome:

```text
backend architecture has survived contact with the real external systems before the UI is built around incorrect assumptions
```

Failures found here are fixed in the responsible earlier domain/backend issues rather than papered over in frontend behavior.

### Phase 4 — Frontend Control Surface

Build the Owner-facing Vue application against the now-proven control-plane contracts.

Includes the settled v1 UX surfaces such as:

- dashboard Task wizard;
- one Task wizard per tab;
- GitHub Issues;
- OpenOrc Tasks;
- Owner Requests;
- Agent Telemetry;
- Activity;
- Configuration;
- Account Settings;
- planning/review/authorization views;
- Reviewer discussion;
- runtime action approvals;
- implementation/PR review status;
- GitHub CI/check projection;
- merge controls;
- durable blocked/recovery presentation;
- centralized transient API-error presentation;
- responsive/mobile behavior;
- PWA posture.

Frontend tests are implemented alongside the UI. The UI consumes authoritative OpenOrc APIs/events and does not become a second workflow engine.

Outcome:

```text
an Owner can operate the normal OpenOrc issue-to-merge lifecycle through the product UI
```

### Phase 5 — Full Product E2E and v1 Hardening

Run the complete human-facing workflow repeatedly and deliberately attack the assumptions most likely to fail under real operation.

This phase includes cross-cutting hardening such as:

- concurrent Tasks within configured runtime capacity;
- shared-Hub and separate-Hub role configurations;
- FIFO queueing/backpressure;
- worker/job replay;
- webhook replay, missed delivery, and out-of-order delivery;
- stale Owner actions/jobs;
- PR-head race handling;
- issue-source changes;
- runtime/session loss;
- provider/Connection failure;
- timeout/uncertain-operation handling;
- cancellation during active execution;
- external PR lifecycle changes;
- audit/history inspection;
- configuration/Connection health;
- deployment/rebuildability;
- security boundary review;
- mobile/PWA sanity checks;
- complete end-to-end regression coverage;
- dogfood readiness.

Correctness is not postponed until this phase. Earlier phases must implement the invariants required for their own capabilities. This phase systematically stress-tests those invariants across the completed system.

Outcome:

```text
OpenOrc v1 is ready for sustained dogfood use
```

## 24.4 Review and implementation loop for each executable issue

Each executable GitHub issue follows the existing manual development loop until OpenOrc is capable of replacing it:

```text
ChatGPT-authored issue
↓
Cline PLAN
↓
Owner relays plan to ChatGPT
↓
ChatGPT reviews against grounding/repository architecture
├── changes required → Owner relays findings to Cline
└── acceptable → Owner authorizes implementation
↓
Cline ACT
↓
Cline commits/pushes and opens/submits the PR through the current manual workflow
↓
ChatGPT reviews the PR/code against issue + repository + architectural intent
├── changes required → Owner relays findings to Cline
└── acceptable → Owner performs the current merge decision
```

The Owner is temporarily the transport between the two AI roles. This manual coordination is dogfooding the workflow OpenOrc itself is intended to automate.

## 24.5 Testing policy during implementation

Testing follows the work rather than being deferred into a separate "test everything later" stage.

Each implementation issue should include the automated tests appropriate to the capability it changes. The repository should accumulate confidence continuously through:

```text
unit/domain tests
persistence tests
adapter contract tests
workflow transition tests
API tests
frontend tests
fake-runtime integration tests
real headless E2E
full UI E2E / dogfood validation
```

Manual E2E is intentionally introduced twice:

```text
first: headless, after the control plane works
→ validates architecture/external integrations early

second: through the finished UI
→ validates the complete Owner experience
```

## 24.6 GitHub milestones and Projects

The implementation graph should not be distorted merely to fit project-management furniture.

GitHub issues, sub-issues, dependencies, labels, PR links, and repository state are the durable implementation record. GitHub Projects may be created if their visual projections are useful to the Owner, but they are not required for correctness, sequencing, or architectural traceability.

GitHub milestones should be used only where they provide a useful release grouping. A single `v1` milestone may be sufficient for the initial implementation, while parent issues and dependencies carry the actual implementation decomposition.

The phases in this section are therefore sequencing guidance, not a requirement to create one GitHub milestone per phase.

## 24.7 Post-v1 work

Post-v1 capabilities such as external engineering-signal intake do not belong in the numbered v1 implementation sequence.

They remain product direction elsewhere in this document and should receive their own implementation decomposition only after the v1 issue-to-PR workflow is reliable and dogfooded.

# 25. Deferred and Permanent Boundaries

## 25.1 Explicitly deferred

Do not solve these before demonstrated need:

- Temporal or another durable workflow engine;
- dedicated OpenOrc Valkey;
- horizontal autoscaling;
- multiple workers by queue type;
- multiple runtime Connections per role / runtime pools;
- pool-level scheduling, failover, dynamic membership, or automatic runtime-fleet provisioning;
- generic assisted deployment into arbitrary Owner-controlled cloud accounts beyond the concrete OpenOrc Cloud-managed Workspace development-environment/ClineBox path;
- multi-region deployment;
- multi-user Workspace membership/invitations;
- enterprise RBAC;
- customer organizations beyond the v1 Workspace boundary;
- exact OpenOrc Cloud Workspace unit price;
- signal-ingestion architecture;
- adopting an existing open PR as a new Task;
- production deployment automation;
- automated E2E;
- runtime marketplace;
- plugin ecosystem.

Future Cline-specific work should be evaluated against the explicit supported Cline release baseline established by Cloud #2 and Core D1 and upgraded deliberately. Changes to remote/SSH runtime mechanics, provider/model or credential APIs, official clients/helpers, and later runtime capabilities trigger targeted compatibility revalidation rather than expansion of OpenOrc's universal runtime contract by assumption.

## 25.2 Permanent anti-goals / guardrails

OpenOrc should resist drifting toward:

- full autonomy as an end in itself;
- removing human approval merely because it appears inefficient;
- binding workflow to a temporary frontier-model ecosystem;
- binding the product architecture to Babelbeez-specific infrastructure;
- treating Workspace isolation as only a billing concern;
- crippling self-hosting to force Cloud adoption;
- maintaining a divergent proprietary Cloud core;
- using one model for every role merely for convenience;
- treating agent activity, token volume, or notification volume as success;
- turning every external signal into a GitHub issue;
- allowing signal-derived work to bypass review/authorization;
- hiding responsibility behind automated decisions;
- becoming another general-purpose coding agent;
- replacing official Agent Runtime implementations;
- building custom runtime worker/spoke infrastructure that belongs to the runtime;
- one runtime VM per Task as a v1 operating model;
- a global single-active-Task restriction or singleton agent session in the core domain;
- re-owning runtime credentials in a universal OpenOrc vault;
- storing raw OpenOrc-owned secrets in ordinary application tables;
- becoming a generic graph/FSM builder with content-free nodes;
- replacing observability or analytics systems;
- replacing GitHub's durable engineering record with proprietary session history;
- hiding large-issue decomposition inside OpenOrc-private phases;
- automatic sensitive-action approval;
- automatic merge in v1;
- cancellation implicitly mutating GitHub artifacts;
- automatic production deployment in v1;
- full IDE functionality in the control-plane UI;
- GitHub-comment-driven Producer authorization;
- Reviewer judgment over uncommitted Producer state;
- free-form Owner ↔ Producer conversation;
- cross-Task role-session reuse;
- implicit session creation/reset/replacement during stage transitions, retries, Executions, or ReviewLoops;
- silently replacing a lost external session;
- OpenOrc reconstructing or owning agent conversational context;
- interpreting free-form discussion as workflow commands or acceptance;
- Workspace guidance redefining protocol, authority, session, or workflow semantics;
- OpenOrc semantically inferring implementation plan drift.

---

# 26. Canonical Implementation Invariants

These are the compact constraints implementation work must preserve.

> A current OpenOrc Task is backed by one authoritative GitHub issue inside one Workspace-scoped Repository, and that issue has at most one current/non-archived Task in that Workspace at a time. Parent/sub-issue hierarchy is mirrored for hierarchy/progress UX only and never implies blocking or non-executability. Task intake re-checks GitHub's authoritative current dependency-blocking state before starting. Both `CANCELLED` and `COMPLETED` Task attempts become archived history; cancellation releases an open issue for a fresh Task, and reopening an issue after a completed/merged attempt likewise permits a fresh Task with fresh role sessions. The same external GitHub repository may be authorized independently in different Workspaces. Large work may be organized through GitHub hierarchy and sequenced through explicit GitHub issue dependencies, not through private OpenOrc execution phases.

> Supabase Auth owns authentication identity; OpenOrc `Profile` is the canonical application identity and ownership reference. `auth.users` is not the ordinary OpenOrc domain user table. In v1 `Profile.id` corresponds 1:1 to the Supabase Auth user UUID.

> GitHub owns durable engineering intent, hierarchy/dependencies, committed code truth, CI/merge policy, and repository outcomes. OpenOrc owns deterministic workflow/control truth.

> GitHub hierarchy is descriptive: parent/sub-issue relationships may be mirrored for context and progress but never determine Task executability. GitHub issue dependency state is authoritative for blocking; OpenOrc does not invent blockers from hierarchy.

> One executable Task has zero or one canonical `TaskPullRequest` in v1. OpenOrc does not automatically replace a closed/unmerged PR with another PR for the same Task. Multiple immutable PR review iterations may reference that one PR, each bound to the exact head SHA reviewed.

> One executable Task produces one Producer-created canonical feature branch and uses an isolated mutable Producer working context. Branch creation/naming is part of the authorized implementation workflow, not a separate OpenOrc lifecycle step. The Producer reports the branch in `implementation_result` as a routing/discovery claim; after push, OpenOrc reconciles it through GitHub before binding the GitHub-confirmed branch to the Task.

> Agent-runtime repository state, including local branches, commits, worktrees, and HEAD SHAs inside a Cline sandbox, is runtime-private execution state and is not canonical OpenOrc engineering truth. The Producer pushes the Task branch; GitHub is authoritative for the durable branch and committed head after push. GitHub webhooks are notifications only; OpenOrc reconciles GitHub API state before using branch/head facts for workflow authority. Runtime-local Git observations may support diagnostics or adapter mechanics but must not substitute for GitHub reconciliation.

> Reviewer acceptance applies to the canonical PR at one exact head SHA. A changed PR head invalidates prior acceptance and requires review of the new head. Movement of the target/base branch alone does not invalidate acceptance; GitHub owns conflict, rebase/update, CI, branch-protection, and merge policy.

> A Reviewer never judges private uncommitted Producer state. Even when Producer and Reviewer share a Cline Hub, Producer and Reviewer use separate Task working contexts; before formal repository review the existing Reviewer session is prepared against the exact committed review subject. In v1 the Reviewer remains in Cline PLAN mode for the Task lifetime; OpenOrc does not recreate Cline's permission system.

> Task boundaries are conversation boundaries. Each Task/role keeps the same external session identity and conversational history from initialization until Task termination; stage changes, retries, Executions, and ReviewLoops are not session boundaries. A runtime may rebuild its internal session/runtime object under the same external `sessionId` and transcript without creating a new OpenOrc Task session.

> Session creation is idempotent for `(Task, role)`. Once a Task/role binding has successfully initialized its external session, that Task has one `TaskAgentSession` binding for the role and its external session identity is never overwritten. A Hub/process outage with recoverable persisted context is not session replacement; genuine loss of the exact context is a blocking/recovery condition. Normal sends never silently create, reset, or replace sessions.

> The OpenOrc Cloud reference topology gives each Workspace using the managed option one persistent Workspace development environment/ClineBox. That environment may contain multiple Workspace repositories and host multiple concurrent isolated Task role sessions subject to configured capacity; it is reused across Tasks rather than recreated per Task. Normal hibernation or destruction is blocked while nonterminal Tasks depend on its live sessions. Hibernation is permitted only after those dependencies end and is a Workspace-environment lifecycle optimization, not Task-session recovery. An explicit emergency destruction may deliberately lose active sessions; affected Task attempts fail closed with session-loss context and cannot resume by silently creating replacements. Continuation uses cancellation/archive plus a fresh Task with fresh role sessions.

> `create_session` succeeds only after fresh Task-isolated context is established and `session_ready` v1 is received and validated.

> The universal v1 session contract is `create_session(Task, role)` plus `send(session_id, message)`. Provider/runtime transport mechanics and runtime-specific controls stay inside adapters.

> Cline v1 uses the official `@cline/sdk` / `ClineCore` remote surface through a replaceable internal backend and thin local Node JSON-RPC subprocess. Supported baseline: CLI `3.0.68`, Core/SDK `0.0.90`. No provider/model catalog, helper Hub, lower-level AgentRuntime integration, private protocol or credential discovery belongs in Core. The bridge owns no workflow semantics or durable state.

> Producer and Reviewer are independently bound persistent Cline sessions. Per-role opaque provider/model IDs and runtime-neutral role Markdown are OpenOrc configuration; Cline owns native authentication/settings. Blank systemPrompt preserves the native harness and config.rules carries role Markdown. The role checkout is both cwd/workspaceRoot; root AGENTS.md remains repository-owned. Reviewer stays PLAN, Producer enters ACT only on authorized control via full-bundle same-ID rebuild. Requested identity is not verified effective readback.

> Self-hosted OpenOrc supports externally managed/BYO Agent Runtimes and does not require OpenOrc to provision them. OpenOrc Cloud may additionally provide Workspace-scoped managed Cline Boxes outside the persistent control-plane process. Managed runtimes are initially provisioned/hydrated, reused across Tasks, may be hibernated by snapshotting and destroying compute only when no nonterminal Task depends on them, and may later be restored; clean reproducible bootstrap remains the fallback rebuild path. Cloud owns host lifecycle and OpenOrc control connectivity, while Cline-native provider/MCP/plugin/tool/sub-agent configuration and credentials remain Cline-owned even on a managed Box. Cross-Workspace runtime sharing is valid only where the runtime itself provides sufficient isolation; OpenOrc never assumes that a project/workspace directory alone supplies that security boundary.

> v1 configures one runtime binding per role, with a Connection plus opaque provider/model selections and optional role-prompt override; NULL uses the shipped default. Roles may share one qualified Hub while retaining separate sessions/working contexts. Owner-configured Connection capacity defaults to 1 and is not Cline-discovered; shared bindings consume the same pool, each required role occupies one slot. Capacity waiting enters QUEUED and advances FIFO; it is not failure. Separate runtime deployments remain valid isolation choices.

> Multiple configured runtimes per role and generic runtime-pool scheduling/failover are post-v1 capabilities. The concrete provision/hibernate/restore/rebuild lifecycle of one configured OpenOrc Cloud-managed Cline Workspace runtime is a hosted capability, not a generic runtime pool. The Task/session domain model must not require redesign when true pools are introduced.

> Runtime telemetry is an optional adapter capability. `UNKNOWN` is a valid agent display state when telemetry is unsupported or unavailable; lack of telemetry is not itself a runtime failure.

> Browser-facing OpenOrc live events use authorized SSE. Native runtime telemetry should be consumed through adapters where available rather than duplicated into Postgres merely for live UI delivery. Only telemetry with independent workflow/audit meaning becomes durable OpenOrc state/history.

> OpenOrc Python operational telemetry uses OpenTelemetry traces, ordinary application logs, and metrics exported through OTLP. This telemetry is observational only and remains distinct from durable `WorkflowEvent` audit history and runtime-owned telemetry. Every API request exposes a safe opaque `request_id` for browser/backend diagnostic correlation without making that identifier workflow authority or idempotency.

> The v1 Vue SPA uses PostHog for explicit product analytics, frontend error tracking, privacy-constrained session replay, and web/performance analytics where useful; browser OpenTelemetry is not required in v1. TanStack Vue Query is the centralized server-state failure-observation seam. Frontend telemetry never captures secrets, prompt/guidance/source/customer content, or arbitrary API payloads, and PostHog availability never affects workflow correctness.

> The review-cleared plan is published through OpenOrc as an Owner-attributed GitHub issue comment rather than by rewriting the authoritative issue body. OpenOrc must not treat that plan publication as an issue-requirement change; OpenOrc retains the Producer/Reviewer provenance while GitHub records the accountable human actor.

> v1 human authentication is GitHub-only through Supabase Auth, while GitHub workflow authorization uses the OpenOrc GitHub App through two distinct boundaries. Workspace/repository routing binds to a GitHub App installation for infrastructure reads, reconciliation, webhook recovery, and capability validation. Owner-accountable mutations that create or change the durable engineering record use a separate Profile-scoped GitHub App user-to-server authorization bound to the same stable GitHub human identity as the Profile's sign-in. The same Profile-scoped authorization is also the v1 authority lineage for HTTP Git performed from that Owner's OpenOrc-managed Cline Boxes; Cloud later owns the hosted token materialization/refresh/revocation mechanics without introducing a per-Workspace OAuth grant or Cline-specific GitHub identity. One physical installation may be represented independently across several users' Workspaces; each human keeps a separate user authorization and remains the GitHub actor for their own writes. Missing/revoked user authorization never falls back to installation-authenticated mutation or PAT.

> GitHub webhooks are inbound change notifications, not a competing source of truth. Deliveries are signature-validated and deduplicated by GitHub delivery identity, then relevant changes are reconciled against authoritative GitHub API state before deterministic workflow consequences are applied. Because GitHub does not automatically redeliver failed webhook deliveries, OpenOrc must retain an independent reconciliation/recovery path for missed notifications.

> Canonical PR creation and every reconciled canonical PR-head change trigger OpenOrc to prepare the Task's existing Reviewer session against the exact current PR/head SHA and issue an exact-subject REVIEW interaction expecting `review_result`. The Reviewer runtime does not independently subscribe to GitHub to decide when review should occur.

> The main dashboard is the primary happy-path execution surface. Each active Task wizard occupies its own tab; the UI must not introduce a global singleton current-Task assumption.

> Agent traffic-light state is scoped to the Task's role session. Runtime session capacity is a separate runtime/Workspace concern and must not be conflated with Task-session health.

> OpenOrc aggregates external state and controls where orchestration requires them, but should link to authoritative native GitHub/runtime/provider interfaces for deeper inspection or management rather than mirror those systems unnecessarily. External links are navigation affordances, not OpenOrc application-data or workflow-control transports.

> Formal v1 agent interactions use the runtime-independent OpenOrc contracts `session_ready`, `plan_result`, `review_result`, `implementation_result`, and `pr_result`. Using the same Agent Runtime for both roles does not merge the role protocols. Adapters parse and validate these contracts and never invent semantic meaning from unstructured prose.

> The LLM is not an idempotency mechanism. OpenOrc/adapter dispatch state plus authoritative runtime reconciliation prevents blind replay of already delivered/completed requests. Known non-delivery failures may receive bounded retries; timeout or uncertain outcomes are reconciled where possible and otherwise block rather than being automatically repeated.

> Workflow-changing operations apply only to the exact current subject/authority context they were created for. Each Task carries an opaque UUID `state_token` that changes on authoritative Task-state mutation; commands/jobs verify that token plus their exact PlanRevision, OwnerGate, PR/head, or equivalent subject before applying effects. If delayed or concurrent work is stale, OpenOrc does not apply it; automatic progression stops and the Owner is given the failure context for recovery. Short Postgres transactions may lock rows while validating/writing, but no transaction remains open across GitHub, Cline, model, or other external calls; unique logical operation identities separately prevent replay of already-applied consequential operations.

> Conversational topology is Owner ↔ Reviewer ↔ Producer. There is no free-form Owner ↔ Producer chat. Owner actions affecting the Producer are typed OpenOrc controls.

> Owner ↔ Reviewer discussion is advisory. Free-form discussion never directly changes workflow state.

> Reviewer outcomes are `ACCEPTED` and `CHANGES_REQUESTED`. Provider/runtime/session/protocol failures travel through a separate adapter error path.

> ReviewLoops default to five Reviewer evaluations per Workspace configuration. Limit exhaustion ends automatic Producer ↔ Reviewer iteration and creates `WAITING_FOR_OWNER / REVIEW_RESOLUTION`. The Owner may Discuss, explicitly Approve/override the unresolved Reviewer objection for that exact subject, or Cancel. Owner override is recorded separately and never fabricates a Reviewer `ACCEPTED`.

> The exact review-cleared PlanRevision is the artifact the human authorizes for implementation. Normal clearance is Reviewer `ACCEPTED`; after ReviewLoop exhaustion it may instead be an explicit Owner `REVIEW_RESOLUTION` override with the unresolved Reviewer objection preserved in history. Its `repository_base_sha` is audit/context metadata, not an automatic invalidation guard; movement of the repository base alone does not invalidate the cleared/authorized plan.

> Once a PlanRevision clears planning, the Task never returns to `PLANNING`. If the plan itself must fundamentally change, the v1 path is cancellation/archive followed by a fresh Task. Implementation changes within the cleared plan may still return from `PR_AUTHORIZATION` to `IMPLEMENTING`, and PR remediation may return from `MERGE_DECISION` to `REVIEWING`.

> If GitHub's authoritative issue requirements change after a Task attempt begins, OpenOrc blocks the attempt and informs the Owner rather than trying to semantically reconcile the changed source of truth. Continuation uses cancellation/archive plus a fresh Task against the updated issue.

> Human implementation authorization and merge decision are explicit deterministic state transitions. Reviewer acceptance does not grant either authority.

> `PR_AUTHORIZATION` applies to the exact latest committed Producer head presented to the Owner. OpenOrc verifies that head immediately before PR creation and reconciles the created PR head immediately afterward because GitHub's PR-creation API has no atomic expected-head guard. A preflight mismatch stops before creation; a post-create mismatch records/preserves the created PR but stops as a stale operation before Reviewer dispatch.

> After PR authorization, the Producer authors only the PR title/body through `pr_result`; OpenOrc owns the actual GitHub PR creation/mutation from the verified canonical Task branch. Repository-level branch creation, commit, and push remain Producer runtime responsibilities.

> A merge request is bound to the exact reviewed or explicitly Owner-overridden PR head. OpenOrc supplies that SHA as GitHub's expected merge SHA so a concurrently changed head cannot satisfy a stale merge decision.

> GitHub CI/check display may require both Checks and commit-status reads for the exact current PR head. OpenOrc presents those GitHub-owned facts but does not duplicate GitHub's branch-protection or merge-policy logic.

> The canonical PR must close its Task's GitHub issue when merged. Producer-authored closing keywords are valid PR content; OpenOrc deterministically ensures the closing reference exists without relying on LLM memory.

> `RuntimeRequest` remains a runtime-neutral scoped interaction model for a demonstrably required adapter contract. No Cline approvals/questions cross the supported v1 seam, so Cline creates no approval RuntimeRequests or callbacks and suppresses ask_question. Any future supported request must remain exact Task/session/request-correlated and typed, never a free-form Owner ↔ Producer channel.

> Where an Agent Runtime exposes supported runtime activity cancel/resume controls, OpenOrc exposes them to the Owner as typed operational controls on the exact bound Task session. Runtime cancel/resume preserves the OpenOrc Task, TaskAgentSession, external session identity, and conversational context and must not be interpreted as Task cancellation, session loss, session reset/replacement, or free-form Owner ↔ agent communication. Exact Execution status/history mapping follows the concrete adapter's supported semantics.

> Cancellation ends OpenOrc orchestration, requests runtime abort for active Producer work, and archives/releases that Task as the current mapping for its GitHub issue. Abort failure/uncertainty is surfaced but does not resurrect the Task or keep the issue locked. Cancellation never implicitly mutates GitHub issues, branches, commits, PRs, or CI/check state.

> Formal JSON schemas, role initialization prompts, and workflow controls are OpenOrc-owned and non-overridable. Role prompts have OpenOrc defaults and optional Owner overrides; their resolved values cannot change during dependent nonterminal Tasks. Workspace guidance is optional Owner-authored prose, blank by default, and may be injected only into OpenOrc-controlled locations. OpenOrc does not persist prompt/guidance versions, hashes, historical snapshots, or per-interaction prompt-use records; formal JSON contracts retain explicit `schema_version` for machine compatibility. Guidance may influence emphasis or conventions but never protocol, authority, session semantics, exact review-subject identity, or state transitions.

> GitHub owns CI and merge policy. OpenOrc displays state, presents the human decision, invokes GitHub, and surfaces the authoritative result.

> Workspace is the isolation boundary for Projects, Repositories, GitHub App installation mappings, Tasks, Agent Runtime Connections/control-endpoint authentication, runtime bindings, policies, and audit state. Profile-scoped GitHub user authorization is deliberately outside that Workspace-owned set so the same human can act through multiple of their own Workspaces without duplicating or sharing credentials across Profiles. In v1, that same human authorization may be usable from multiple managed Cline Boxes and may effectively reach repositories associated with several of the Profile's Workspaces; this is an explicit accepted exception and does not merge those Workspaces' OpenOrc/runtime state. A physical Agent Runtime may be shared across Workspaces only where its own non-GitHub credential/configuration/working-context isolation preserves that boundary; otherwise separate runtime isolation domains are required.

> Authentication ownership follows direct consumption and runtime ownership. Externally managed runtimes keep their provider/MCP/tool/account/Git credentials outside OpenOrc. On an OpenOrc Cloud-managed Cline Box, Cline-native provider/MCP/tool credentials remain Cline-owned, while Git uses Core's existing Profile-scoped Owner GitHub App authorization. Cloud owns only the downstream hosted mechanics required to make that authorization usable on the Box once deployed Core exists; no per-Workspace OAuth grant, Git proxy, generic credential framework, or Cline-Box-specific Core abstraction is introduced in v1. Other managed infrastructure/control secrets remain Workspace-scoped behind the secure secret boundary and are never exposed in prompts, telemetry, or ordinary APIs.

> Workflow state belongs in Postgres, not in ephemeral RQ jobs or agent transcripts. OpenOrc application persistence uses direct Python Postgres access against Supabase Postgres with explicitly bounded/configurable process pools; browser application data never bypasses the OpenOrc API to use Supabase Data APIs directly. Connection/pooler mode is deployment configuration, not domain architecture. The control plane does not require persistent local filesystem state.

> OpenOrc application tables live in a dedicated `openorc` Postgres schema. Domain state/type vocabularies use Python enums/value objects with database text/CHECK constraints; real-world instants use `TIMESTAMPTZ` and timezone-aware Python datetimes normalized to UTC; merged migrations are append-only. Historical evidence is immutable after finalization, current-object pointers identify rather than duplicate related state, and every semantic fact has one canonical storage home.

> Normal workflow lifecycle archives and retains OpenOrc history. Explicit Owner administrative purge may remove OpenOrc-owned internal records according to deliberate aggregate deletion rules, but purging OpenOrc data never implicitly deletes or mutates GitHub issues, branches, commits, PRs, checks, or other durable GitHub engineering artifacts.

> `openorc/core` is the complete open-source product and `openorc/cloud` depends on core for hosted/reference operation; core never depends on Cloud. The starting repository structure should make deployment surfaces, shared services/domain code, adapters, persistence, hosted-only billing, and Cloud deployment infrastructure visibly separate. API routers and worker jobs remain thin transports over shared services rather than owning business logic.

> Self-hosted OpenOrc and OpenOrc Cloud use the same core product. Cloud billing retains the fixed recurring active-Workspace charge through Polar; optional OpenOrc-managed Workspace development-environment compute is separately metered by running compute time. A hibernated runtime does not accrue running-compute time; retained snapshot/storage treatment, exact commercial rates, and rounding remain Cloud commercial configuration outside Core workflow semantics.

> Automatic detection or issue creation never implies implementation authorization. Signal-derived work enters the same governed workflow as human-originated work.

---
