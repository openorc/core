# OpenOrc Repository Bootstrap and AGENTS.md Cascade Specification

**Purpose:** bootstrap `openorc/core` and `openorc/cloud` from the settled OpenOrc grounding document without forcing Cline, ChatGPT, or future contributors to reconstruct the architecture from scratch.

**Primary design source:** OpenOrc Grounding Document  
**Reference cascade studied:** `babelbeez/Babelbeez` at commit `f70a269234532c0c825236ae9a22454d111516cc`  
**Additional bootstrap input:** current Cline global `default-rules.md` supplied by the Owner

This document is **not** another product specification. The grounding document remains the source for product/workflow semantics during early implementation. This document exists to compile that design into repository-native operating context:

```text
Grounding Document
        ↓
this bootstrap specification
        ↓
repository structure
README.md
AGENTS.md cascade
test/docs scaffolding
        ↓
GitHub implementation issues
        ↓
code/schema/config progressively become authoritative
```

Once the repositories are bootstrapped and the `AGENTS.md` hierarchy exists, this file becomes a transitional bootstrap artifact rather than a competing source of truth.

---

# 1. What We Learned from the Babelbeez Cascade

Babelbeez currently has thirteen `AGENTS.md` files:

```text
AGENTS.md
├── .github/AGENTS.md
├── deployment/AGENTS.md
├── integrations/
│   ├── make/AGENTS.md
│   └── wordpress/AGENTS.md
├── resources/AGENTS.md
├── routers/AGENTS.md
├── sdk/AGENTS.md
├── services/AGENTS.md
├── src/
│   ├── AGENTS.md
│   ├── js/
│   │   └── sdk/AGENTS.md
│   └── v2/AGENTS.md
└── supabase/AGENTS.md
```

The useful pattern is not “put an `AGENTS.md` in every directory.” The useful pattern is:

```text
root context
→ nearest local context
→ explicit cross-boundary context traversal when work spans subsystems
```

The best Babelbeez files consistently answer five questions:

1. **What does this subtree own?**
2. **What does it explicitly not own?**
3. **What invariants must never be violated here?**
4. **What is the source of truth?**
5. **Which adjacent `AGENTS.md` files must be read for cross-boundary changes?**

The strongest reusable principle is therefore:

> `AGENTS.md` should primarily encode boundary + invariant + authority + forbidden shortcut.

It should not become a chronological architecture journal, issue history, or miniature grounding document.

Babelbeez also demonstrates the main failure mode to avoid: mature subsystem files can accumulate issue-specific history, provider quirks, migration stories, and detailed feature architecture until they become difficult to maintain. OpenOrc should split local context when a genuine nested subsystem appears rather than allowing `services/AGENTS.md` or root to become dumping grounds.

---

# 2. OpenOrc Context Layers

OpenOrc implementation will operate with several context layers. They must remain distinct.

## 2.1 Cline workspace / harness rules

These are rules that apply to the **development Cline workspace**, across the repositories opened in that workspace, without becoming portable repository policy.

The previous global `default-rules.md` model has been retired. Babelbeez now carries its development-Cline rules as workspace-level `babelbeez-rules.md`, and the OpenOrc VS Code workspace should receive the corresponding `openorc-rules.md`.

Examples from the current Owner-managed rule set include:

- how Cline loads inherited `AGENTS.md`;
- local `activeTask.md` behavior;
- when Cline considers updating `AGENTS.md`;
- patch-editing safety;
- `attempt_completion` safety;
- preferred documentation/web-fetch tooling;
- generic Supabase migration safety;
- current Owner preferences for frontend visual verification;
- generic branch/PR hygiene.

These rules belong in the OpenOrc development VS Code/Cline workspace as `openorc-rules.md`, not copied wholesale into every repository.

A future product Runtime Hub may use a separately curated repo-agnostic baseline derived from some of these rules, but that is a different configuration surface.

## 2.2 Repository root `AGENTS.md`

The root guide owns repository-wide implementation context:

- what this repository is;
- where it sits in the OpenOrc architecture;
- global sources of truth;
- dependency direction;
- global implementation invariants;
- repository governance actually configured for this repository;
- testing/security baseline;
- the map of nested context perimeters.

The root guide must remain an entry vector, not the complete product specification.

## 2.3 Nested subsystem `AGENTS.md`

Nested guides own local implementation constraints.

They should contain only rules that are:

1. local to the subtree; and
2. expensive, dangerous, or architecture-corrupting for an implementation agent to miss.

Nested guidance specializes root guidance rather than repeating it.

## 2.4 GitHub issue context

Executable GitHub issues own task-specific work:

- capability to implement;
- issue-local acceptance criteria;
- required tests;
- dependencies;
- explicit task non-goals;
- architectural constraints particularly relevant to that issue.

Issues should not repeat the full repository architecture.

## 2.5 `activeTask.md`

The current Cline global rules define root-level `activeTask.md` as optional local working context.

For OpenOrc repositories:

- it remains local scratch context only;
- it is not architecture documentation;
- it is not a source of product truth;
- it should never be required for another clone to understand the repository;
- if used, it should be ignored by Git and never committed.

The bootstrap should therefore add root-level `activeTask.md` to `.gitignore`.

## 2.6 README and detailed documentation

`README.md` explains the product/repository and how to get started.

Detailed operational/test/development documentation may live in ordinary Markdown files such as:

```text
tests/README.md
deployment/README.md
docs/...
```

`AGENTS.md` should point to those documents where necessary rather than absorbing long command references, tutorials, or historical explanation.

---

# 3. Authority and Precedence During Bootstrap

During early implementation:

```text
Grounding Document
→ canonical product/design intent where repository artifacts have not yet encoded it

AGENTS.md
→ durable repository-native implementation constraints

GitHub issue
→ scope and acceptance criteria for one implementation task

code/schema/tests/config
→ authoritative implemented behavior once established
```

When implementation changes a durable rule or boundary, the relevant `AGENTS.md` must be updated so repository guidance does not drift behind the code.

The grounding document must not be mechanically copied into root or nested guides. Its job is to provide the raw design truth from which concise repository rules are extracted.

---

# 4. Important Distinction: Build-Time Cline vs Product Runtime Cline

There are two very different Cline contexts in OpenOrc's life.

## 4.1 The Cline currently building OpenOrc

This is the Owner's development Cline running in the OpenOrc VS Code workspace.

The old global `default-rules.md` has been retired. The OpenOrc workspace should instead carry an explicit workspace-level `openorc-rules.md`, analogous to the new Babelbeez workspace-level `babelbeez-rules.md`.

That Cline:

- receives GitHub issues from the Owner/ChatGPT workflow;
- creates normal implementation branches;
- currently opens/submits PRs itself;
- follows the Owner's existing manual PR workflow;
- asks the Owner when the global rules require confirmation;
- uses the current local developer tooling and MCP configuration.

This is **bootstrap/development process**, not OpenOrc product semantics.

## 4.2 Cline Hubs orchestrated by OpenOrc

These are Product-side Agent Runtime Connections used as Producer and Reviewer.

Their behavior is governed by:

```text
Cline runtime/harness baseline
+
OpenOrc initialization layer
+
role-specific Producer or Reviewer instructions
+
OpenOrc formal protocol envelopes
+
repository AGENTS.md cascade
```

The current development `default-rules.md` must **not** be copied blindly into these runtime Hubs.

Several current global rules would conflict with OpenOrc product semantics:

- the current rule expects Cline to submit PRs;
  - in OpenOrc v1, after `PR_AUTHORIZATION`, the Producer authors only `pr_result`;
  - OpenOrc creates/mutates the actual GitHub PR.
- the current rule assumes the user will monitor PR CI and report problems;
  - OpenOrc itself reconciles GitHub check state for workflow presentation.
- the current rule hard-codes merge-commit behavior;
  - OpenOrc explicitly treats merge method, update/rebase requirements, branch protection, and merge eligibility as GitHub/repository policy.
- the current rule tells Cline to rebase a behind PR branch as generic behavior;
  - OpenOrc must not hard-code repository-specific update policy into universal workflow semantics.
- the current rule allows Cline to ask the user before updating local files such as `AGENTS.md`;
  - OpenOrc's Producer has no free-form Owner chat channel. Product runtime interaction must remain inside OpenOrc's typed protocol and RuntimeRequest boundaries.
- the current global frontend verification behavior reflects the Owner's present development process;
  - it is not a permanent OpenOrc runtime invariant.
- `attempt_completion` and `apply_patch` rules are harness-specific operational safety, not repository architecture.

Therefore:

> The build-time Cline global file is an input to future Hub configuration, not the future Hub configuration itself.

The safe repo-agnostic subset may eventually become part of reference Cline Hub provisioning, but OpenOrc correctness must never depend on those runtime-global rules being present. Product semantics are enforced by OpenOrc protocol, domain/application logic, adapter validation, GitHub authority, and repository context.

---

# 5. Disposition of the Current Development Cline Rules

The old global `default-rules.md` no longer exists as a global configuration surface.

Its Babelbeez successor is `babelbeez-rules.md` at that VS Code/Cline workspace level. OpenOrc should receive its own `openorc-rules.md` at the equivalent workspace level.

This is the desired scope split:

```text
Cline installation/global layer
→ minimal or empty repo-specific policy

OpenOrc VS Code workspace
→ openorc-rules.md
→ development-harness workflow/tool preferences for building OpenOrc

openorc/core and openorc/cloud repositories
→ AGENTS.md cascades
→ portable repository architecture and engineering constraints

future OpenOrc-managed Cline Hub
→ separately curated runtime-Hub baseline
+ OpenOrc initialization / Producer / Reviewer protocol context
```

The current rule content should be classified as follows.

| Current rule area | Keep in `openorc-rules.md` | Copy into OpenOrc `AGENTS.md` | Candidate for reference runtime Hub baseline | Notes |
| --- | --- | --- | --- | --- |
| Context loading / inherited `AGENTS.md` | Yes | No duplication; root provides perimeter map | Yes | Pure harness behavior |
| `activeTask.md` | Yes | Only repo `.gitignore` / optional local note | Usually no | Product Tasks already have runtime session context; avoid cross-Task scratch leakage |
| Cross-boundary context traversal | Yes | Root and local files define actual boundaries | Yes | Harness rule + repo graph complement one another |
| `AGENTS.md` / `README.md` maintenance | Yes, rewritten | No duplication | Yes, rewritten | Task-scoped documentation maintenance is allowed; product runtime must not require free-form Owner confirmation |
| Generic task branches / PRs | Yes for current build workflow | Root records actual repo policy only | No, not verbatim | Product workflow owns when PR creation occurs |
| PR labels when repo automation defines them | Yes | `.github/AGENTS.md` owns actual taxonomy if one exists | No universal rule needed | No taxonomy should be invented at bootstrap |
| Merge-commit-only rule | Current environment only | **Do not copy unless actually configured** | No | Not repo-agnostic; OpenOrc treats merge policy as GitHub/repo-owned |
| Automatic rebase-on-behind rule | Current environment only | **Do not copy unless actually configured** | No | Same reason |
| New behavior requires tests / bug fixes require regression tests | Yes | Yes, as repo testing baseline | Yes conceptually | Durable engineering quality rule |
| Frontend no-browser/manual review preference | Yes for current Owner workflow | Do not fossilize as product architecture | Usually no | Automated E2E is currently deferred, but this is still a process preference |
| `attempt_completion` command prohibition | Yes | No | Only if same harness/API exists | Tool-specific safety |
| `apply_patch` update-vs-delete/add safety | Yes | No | Only if same patch tool exists | Tool-specific safety |
| Generic Supabase migration least privilege | Yes | `supabase/AGENTS.md` adds OpenOrc specifics | Maybe | Safe generic database rule |
| Web/documentation fetch priority | Yes | No | Yes if same tools exist | Tooling policy, not repo architecture |

When creating `openorc-rules.md`, do not mechanically copy the two Babelbeez-style repository assumptions:

```text
PRs merge via merge commit
behind PR branches are generically rebased
```

and replace them with:

```text
follow the repository's configured merge/update policy from its AGENTS.md and GitHub settings;
do not invent a merge strategy when the repository has not established one.
```

Until then, OpenOrc root guidance should explicitly establish that repository/GitHub policy overrides generic merge-method assumptions.

---

# 6. Rule Placement Principles for OpenOrc

## 6.1 Root bloat guardrail

Root `AGENTS.md` should contain:

- product identity;
- repo identity;
- global authority boundaries;
- global architecture/dependency direction;
- global safety/testing rules;
- context perimeter map.

It should not contain:

- the whole workflow state machine;
- every JSON protocol schema;
- every Cline SDK mechanic;
- every GitHub API reconciliation detail;
- every UI screen;
- every database table;
- every deployment field.

Those belong in local guides, code, schema, tests, or the grounding document.

## 6.2 Local guide template

Most nested files should follow a stable shape:

```markdown
# <Subsystem> Agent Context

## Subsystem Boundary
What this subtree owns and does not own.

## Core Invariants
Rules that must not be violated.

## Sources of Truth
Which code/data/system is authoritative.

## Forbidden Shortcuts
Common tempting implementations that would violate architecture.

## Testing Expectations
Local test implications where useful.

## Cross-Boundary Checks
Which other AGENTS.md files must be read when this subsystem affects them.
```

Not every file needs every heading, but this is the preferred pattern.

## 6.3 Add a new nested guide only when justified

Add `AGENTS.md` only when a subtree has at least one durable local rule whose absence is likely to cause architectural damage, security risk, expensive rework, or repeated confusion.

Do not mirror directory structure for aesthetics.


## 6.4 Anti-bloat through separation of concerns

The bootstrap document may be exhaustive. The generated repository guides should be concise because they contain only **operating context**, not because they are constrained by arbitrary line limits.

Do not restrict Cline to a fixed number of `AGENTS.md` files. Cline should read whatever inherited and cross-boundary context is relevant to the task.

The preferred distinction is:

```text
AGENTS.md
→ durable rules, boundaries, invariants, authority, forbidden shortcuts,
  context-traversal instructions

README.md
→ explanatory documentation, setup, commands, examples, test organization,
  troubleshooting, architecture notes, operational detail
```

It is completely valid, and often desirable, to have both files in the same directory.

Example:

```text
tests/
├── AGENTS.md
└── README.md
```

`tests/AGENTS.md` might say:

```text
- Read the AGENTS.md for the subsystem under test.
- Ordinary tests must not require live external infrastructure.
- Bug fixes require regression coverage.
```

`tests/README.md` can then be much longer and explain:

- test suite layout;
- commands;
- markers;
- fixtures;
- fake adapters;
- integration-test setup;
- naming conventions;
- debugging techniques;
- expected local dependencies.

That explanatory detail does not need to be loaded as standing agent policy on every task, but remains available when the task actually needs it.

The same pattern applies elsewhere:

```text
deployment/
├── AGENTS.md   # deployment invariants and authority
└── README.md   # deployment procedure and operator documentation

packages/cline-sdk-bridge/
├── AGENTS.md   # containment boundary and forbidden logic
└── README.md   # protocol, development, testing, troubleshooting
```

## 6.5 Duplication rule

Do not repeat a rule at every level merely for emphasis.

The cascade should behave like inheritance:

```text
workspace rule
+ root rule
+ local specialization
```

A child guide should mention a parent rule again only when it needs to narrow, strengthen, or clarify that rule locally.

Cross-boundary sections should point to other applicable guides rather than restating their contents.

## 6.6 Task-scoped documentation maintenance

`AGENTS.md` and `README.md` are normal repository artifacts.

When a task changes durable repository behavior, Cline may update the relevant documentation as part of the same task branch/PR without seeking a separate Owner confirmation.

Use `AGENTS.md` for changed durable operating rules, boundaries, authority, security constraints, workflow expectations, or context traversal.

Use `README.md` for changed setup, commands, examples, test organization, troubleshooting, explanatory architecture, or operating procedures.

Do not update either file for temporary debugging notes or task-local trivia.

In the future OpenOrc-managed Producer flow, documentation maintenance must not depend on a free-form Owner question. Human review occurs through the normal OpenOrc/GitHub review and authorization path.

## 6.7 Compression during bootstrap

The detailed subsystem sections in this bootstrap specification are **source material**, not templates to paste verbatim.

When generating the real `AGENTS.md` files:

- collapse explanatory prose into short operational rules;
- move setup, commands, examples, walkthroughs, and troubleshooting into nearby `README.md` files where useful;
- remove rationale already captured in ordinary documentation;
- preserve invariants, ownership boundaries, authority, and forbidden shortcuts;
- add cross-boundary pointers where another local guide must be consulted;
- do not remove useful context merely to satisfy a length target.

The generated cascade should be smaller than this source because explanation and operating policy are separated, not because Cline is being artificially starved of context.

# 7. Recommended `openorc/core` Repository Shape

The grounding document already defines the starting structural intent. The bootstrap should instantiate it as:

```text
openorc/core/
├── .github/
│   ├── AGENTS.md
│   └── workflows/
│
├── apps/
│   ├── app/
│   │   └── AGENTS.md
│   ├── api/
│   └── worker/
│
├── src/
│   └── openorc/
│       ├── AGENTS.md
│       ├── domain/
│       │   └── AGENTS.md
│       ├── services/
│       │   └── AGENTS.md
│       ├── api/
│       │   ├── AGENTS.md
│       │   ├── routers/
│       │   ├── schemas/
│       │   └── dependencies/
│       ├── workers/
│       │   ├── AGENTS.md
│       │   └── jobs/
│       ├── adapters/
│       │   ├── AGENTS.md
│       │   ├── github/
│       │   │   └── AGENTS.md
│       │   └── cline/
│       │       └── AGENTS.md
│       └── persistence/
│           └── AGENTS.md
│
├── packages/
│   └── cline-sdk-bridge/
│       └── AGENTS.md
│
├── supabase/
│   ├── AGENTS.md
│   └── migrations/
│
├── tests/
│   ├── AGENTS.md
│   └── README.md
│
├── docs/
├── AGENTS.md
├── README.md
├── LICENSE
└── .gitignore
```

This is slightly more complete than the grounding document's illustrative tree because the architecture is already detailed enough to justify two adapter-local guides and a test-local guide from day one.

The following intentionally do **not** get initial guides:

- `apps/api/`;
- `apps/worker/`;
- individual `api/routers/`;
- individual `workers/jobs/`;
- `docs/`.

`apps/api` and `apps/worker` should remain intentionally boring bootstrap surfaces. If they ever need large local rule sets, that is itself a warning that business behavior may be leaking into process entrypoints.

---

# 8. `openorc/core/AGENTS.md`

The root guide should be concise but strong.

## 8.1 Product baseline

Seed content:

- OpenOrc is an open, provider-neutral control plane for governed agentic software development.
- It orchestrates workflow; it does not replace coding runtimes, models, GitHub, CI, or observability systems.
- The durable product is the engineering control layer: GitHub-backed Tasks, reviewed intent, explicit authority, Task-scoped agent sessions, independent review, runtime requests, deterministic workflow state, PR/CI/merge coordination, and audit history.
- Human attention is scarce; automate mechanical coordination between explicit consequential gates.

## 8.2 Repository identity

State explicitly:

- `openorc/core` is the complete Apache-2.0 open-source product.
- Self-hosting must remain complete.
- `openorc/cloud` may depend on core.
- core must never import, depend on, or require Cloud-only code.
- Supabase persistence/auth integration is supported product code in core.
- Cline v1 support is an official runtime adapter in core.
- concrete OpenOrc Cloud infrastructure and Polar billing do not belong here.

## 8.3 Global sources of truth

Root should establish:

```text
GitHub
→ durable engineering intent, issue hierarchy/dependencies, committed code, PRs, CI/checks, branch protection, merge policy/outcomes

OpenOrc/Postgres
→ workflow/control truth, Tasks, sessions bindings, reviews, gates, executions, runtime requests, block state, audit/workflow events

Agent Runtime
→ conversational/private runtime state, model/provider/tool execution state owned by the runtime
```

GitHub webhooks are notifications requiring authoritative reconciliation, not truth themselves.

Agent transcripts are not durable OpenOrc workflow state.

## 8.4 Global architecture rules

Root-level invariants should include:

- API routers and RQ jobs are thin transports over shared application services.
- workflow/domain semantics belong in `src/openorc/domain/` and `src/openorc/services/`.
- adapters own external mechanics/normalization, not workflow meaning.
- workflow state belongs in Postgres, never ephemeral RQ jobs or agent transcripts.
- browser-facing live OpenOrc updates use authorized SSE.
- runtime telemetry is optional adapter capability and is not automatically durable workflow history.
- OpenOrc does not build a universal provider credential vault.
- core must remain provider/runtime neutral above adapter boundaries.
- no Cloud-specific infrastructure or billing assumptions may leak into core.
- use official runtime/provider abstractions rather than reimplementing their internal protocols where the official abstraction meets the contract.

## 8.5 Global workflow invariants worth keeping at root

Only the most cross-cutting ones:

- one GitHub issue has at most one current/non-archived OpenOrc Task;
- Task boundaries are conversation boundaries;
- Producer and Reviewer are separate logical roles with separate Task-scoped sessions/working contexts;
- Owner ↔ Reviewer ↔ Producer is the allowed conversational topology; no free-form Owner ↔ Producer chat;
- human implementation authorization and merge decisions are explicit;
- prompt customization may change instructions, never protocol/authority/state-machine semantics;
- stale workflow-changing operations must not be applied to newer subjects;
- cancellation ends OpenOrc orchestration without implicitly mutating GitHub artifacts;
- exact repository review is commit/PR-head addressed;
- OpenOrc never silently replaces a lost Task role session.

Detailed transition mechanics belong in domain/services guidance and tests.

## 8.6 Repository workflow

Do not copy the current Babelbeez workspace merge strategy into repository architecture merely because it exists in `babelbeez-rules.md`.

The root file should say:

- normal repository work occurs on task branches via PRs to protected `main`;
- agents do not merge PRs unless repository policy is explicitly changed later;
- follow the actual GitHub branch protection, update, and merge policy configured for this repo;
- do not assume merge commit, squash, rebase-merge, or branch-update strategy merely because another repository uses it;
- new behavior ships with tests in the same PR;
- bug fixes ship with regression tests;
- repository-specific labels/required checks should be documented in `.github/AGENTS.md` once established.

## 8.7 Security baseline

- never commit secrets;
- Workspace isolation is a security boundary, not merely billing scope;
- authentication ownership follows direct consumption;
- runtime/provider/MCP/tool credentials owned by Cline remain runtime-owned;
- raw OpenOrc-owned secrets do not belong in ordinary application tables;
- sensitive values must not be logged;
- fail closed at authorization/authority boundaries.

## 8.8 Context perimeter map

Root should explicitly list:

- `apps/app/AGENTS.md`;
- `src/openorc/AGENTS.md`;
- `src/openorc/domain/AGENTS.md`;
- `src/openorc/services/AGENTS.md`;
- `src/openorc/api/AGENTS.md`;
- `src/openorc/workers/AGENTS.md`;
- `src/openorc/adapters/AGENTS.md`;
- `src/openorc/adapters/github/AGENTS.md`;
- `src/openorc/adapters/cline/AGENTS.md`;
- `src/openorc/persistence/AGENTS.md`;
- `packages/cline-sdk-bridge/AGENTS.md`;
- `supabase/AGENTS.md`;
- `tests/AGENTS.md`;
- `.github/AGENTS.md`.

When a task spans boundaries, read every relevant local guide before editing.

## 8.9 Root bloat rule

End with an explicit instruction:

> Keep root as the repository entry vector. Local implementation rules belong in the nearest relevant `AGENTS.md`; task-specific detail belongs in GitHub issues; detailed product semantics remain in the grounding/design source until code and repository-native contracts replace them.

---

# 9. `openorc/core/src/openorc/AGENTS.md`

This file governs the shared Python control-plane package.

## 9.1 Boundary

It owns application/domain/backend implementation shared by API and worker process surfaces.

It does not own:

- Vue UI;
- Cloud deployment;
- Polar billing;
- Cline SDK Node compatibility mechanics;
- process-specific startup behavior except shared reusable support.

## 9.2 Dependency direction

Preferred conceptual direction:

```text
domain
↑
services
↑
API / workers

services
→ persistence interfaces/implementations
→ external adapters

adapters/persistence
→ external systems
```

Do not allow:

```text
domain → FastAPI
domain → RQ
domain → GitHub SDK
domain → Cline SDK
domain → Supabase SDK

services → FastAPI DTOs
services → RQ job objects

routers/jobs → duplicate business logic
```

Exact module/interface implementation may evolve, but the semantic direction must remain.

## 9.3 Shared Python rules

- domain/services must be usable without an HTTP request or RQ job object;
- expected domain/application failures should use typed internal errors rather than leaking provider/transport exceptions to every caller;
- transport layers translate internal errors into their own wire/job behavior;
- workflow meaning is never inferred from database shape or provider response shape;
- all Workspace-scoped operations must preserve explicit Workspace isolation;
- exact current subject/authority context must be validated before workflow-changing effects;
- external operation uncertainty is not success or known failure.

## 9.4 Cross-boundary checks

Changes here may require reading:

- `domain/AGENTS.md`;
- `services/AGENTS.md`;
- API/worker local guidance;
- relevant adapter guidance;
- persistence/Supabase guidance.

---

# 10. `openorc/core/src/openorc/domain/AGENTS.md`

This is the semantic heart of the product.

## 10.1 Boundary

Own:

- entities/value objects;
- state enums;
- state-transition rules;
- exact-subject invariants;
- authority semantics;
- domain validation independent of transport/provider implementation.

Do not import:

- FastAPI;
- RQ;
- Supabase SDK;
- GitHub SDK;
- Cline SDK;
- Node bridge mechanics;
- Cloud billing/deployment code.

## 10.2 v1 domain model

The guide should name the adopted concepts so Cline does not invent parallel abstractions:

- Workspace;
- Project;
- Repository;
- Connection;
- role/runtime binding;
- Task;
- TaskAgentSession;
- PromptTemplateOverride / effective prompt configuration reference;
- PlanRevision;
- ReviewLoop;
- ReviewIteration;
- OwnerGate;
- Execution;
- RuntimeRequest;
- TaskBlock;
- TaskPullRequest;
- WorkflowEvent.

Do not invent speculative v1 domain entities merely to make code feel more “generic.”

## 10.3 Core Task invariants

- one GitHub issue has at most one current non-archived Task;
- cancellation archives/releases the current mapping so the issue may back a fresh Task;
- executable workflow runs on leaf Tasks;
- composite Tasks are tracking/orchestration containers;
- one executable Task has one Producer-created canonical branch;
- one executable leaf Task has at most one canonical current PR.

## 10.4 Session invariants

- one Task/role has one persistent external session identity for Task lifetime;
- session creation is idempotent for `(Task, role)`;
- stage transitions, retries, Executions, and ReviewLoops do not create sessions;
- missing/lost session is a blocking/recovery condition, never silent replacement;
- fresh Task means fresh role sessions;
- Producer and Reviewer working contexts remain separate even on the same physical Hub;
- Reviewer remains logically independent from Producer.

## 10.5 Review and authority invariants

- Reviewer outcomes are `ACCEPTED` and `CHANGES_REQUESTED`;
- runtime/provider/protocol failures are not Reviewer judgments;
- acceptance attaches to the exact subject being reviewed;
- PR acceptance is bound to exact PR head SHA;
- target/base movement alone does not invalidate acceptance;
- a changed PR head does;
- Reviewer acceptance never grants implementation or merge authority;
- Owner override after exhausted ReviewLoop is recorded separately and never fabricates Reviewer acceptance;
- free-form Owner ↔ Reviewer discussion does not itself change workflow state.

## 10.6 Planning/state-machine invariants

- the exact cleared PlanRevision is what the Owner may authorize;
- once planning clears, v1 never returns to `PLANNING`;
- fundamental plan replacement requires cancel/archive + fresh Task;
- changed authoritative GitHub issue requirements after Task attempt start block the attempt rather than being semantically merged;
- workflow-changing operations must match the current exact subject/gate/authority context;
- stale delayed work stops automatic progression for Owner recovery.

## 10.7 RuntimeRequest invariants

- runtime-originated consequential approvals are scoped/correlated to exact Task/session/request;
- approve/reject is a typed control;
- it is not a general Owner ↔ Producer chat channel;
- general Producer `ask_question` / `OWNER_DECISION` conversation is not part of v1.

## 10.8 Cancellation

- cancellation ends OpenOrc orchestration;
- cancellation may request runtime abort;
- abort uncertainty/failure is surfaced but does not resurrect Task orchestration;
- cancellation does not delete or mutate GitHub issue/branch/commit/PR/CI artifacts by implication.

## 10.9 Cross-boundary checks

Any domain change affecting storage, API contracts, adapters, or UI must also read the corresponding local guides.

---

# 11. `openorc/core/src/openorc/services/AGENTS.md`

Application services own deterministic use-case orchestration.

## 11.1 Boundary

Services:

- apply domain rules;
- load/update durable state;
- validate current Task/subject/authority context;
- call persistence and adapter boundaries;
- decide deterministic workflow consequences;
- enqueue/follow up where required.

Services do not own:

- FastAPI request/response DTOs;
- RQ job semantics;
- Cline SDK wire mechanics;
- GitHub wire mechanics;
- Vue state;
- Cloud infrastructure.

## 11.2 Canonical call chain

Preserve:

```text
FastAPI request / RQ job
↓
application service
↓
domain/current-subject checks + durable state
↓
adapter/persistence operation
↓
external system when required
```

Return:

```text
external system
↓
adapter normalized result/error
↓
application service
↓
deterministic workflow consequence
↓
Postgres + follow-on job/event if needed
```

## 11.3 Exact-subject safety

Before any delayed, replayed, or asynchronous workflow-changing action:

- reload authoritative current Task state;
- verify the exact subject/OwnerGate/Execution/PR-head/request context still matches;
- if stale, do not apply the effect;
- persist/surface recovery context rather than guessing.

This is mandatory for job replay and delayed Owner actions.

## 11.4 External side-effect semantics

Services must distinguish:

```text
known success
known non-delivery/failure
uncertain outcome
```

Never convert timeout/connection loss automatically into “safe to retry.”

Where possible:

- reconcile against authoritative provider/runtime state;
- retry only known non-delivery under bounded policy;
- block on unresolved uncertainty.

The LLM is never the idempotency mechanism.

## 11.5 ReviewLoop/application behavior

- normal Producer ↔ Reviewer iteration remains automatic until configured limit;
- default limit is five Reviewer evaluations;
- exhaustion creates `WAITING_FOR_OWNER / REVIEW_RESOLUTION`;
- Owner options are Discuss, exact-subject Approve/override, or Cancel;
- Owner override preserves unresolved Reviewer objection;
- Owner discussion remains advisory until a typed action occurs.

## 11.6 GitHub/publication behavior at service level

Services, not routers/adapters alone, decide when the workflow is allowed to:

- publish the review-cleared plan comment;
- request PR composition;
- create/reconcile the canonical PR;
- dispatch Reviewer PR review;
- invoke merge.

Adapter files provide mechanics; service/domain logic provides permission and workflow timing.

## 11.7 Cross-boundary checks

Read:

- `domain/AGENTS.md` for semantic changes;
- adapter guide(s) for external behavior;
- persistence/Supabase guides for durable model changes;
- API/worker guides for transport changes.

Do not let this file grow into feature-by-feature architecture history. Split a real service subsystem only when it develops durable local invariants of its own.

---

# 12. `openorc/core/src/openorc/api/AGENTS.md`

FastAPI is an application transport, not the workflow engine.

## 12.1 Boundary

Own:

- HTTP routing;
- auth/session dependency extraction;
- request parsing/validation;
- response serialization;
- wire DTOs;
- HTTP error mapping;
- authorized SSE endpoints;
- transport-only concerns.

Do not own:

- workflow transitions;
- review logic;
- session lifecycle decisions;
- GitHub reconciliation policy;
- retry/idempotency policy;
- persistence rules;
- direct Cline/GitHub orchestration bypassing services.

## 12.2 Rules

- routers call shared application services;
- do not duplicate business/domain validation that must also apply to workers or future transports;
- browser API clients never call Agent Runtimes directly;
- runtime/provider links exposed to UI are navigation affordances only;
- SSE is live delivery, not durable truth;
- authentication/Workspace scope must be resolved before application operations;
- API schemas changing frontend contracts require reading `apps/app/AGENTS.md`.

## 12.3 Cross-boundary checks

API contract changes may require:

- `services/AGENTS.md`;
- `domain/AGENTS.md`;
- `apps/app/AGENTS.md`;
- persistence/Supabase guidance.

---

# 13. `openorc/core/src/openorc/workers/AGENTS.md`

RQ is execution machinery, not canonical workflow state.

## 13.1 Boundary

Own:

- job entrypoints;
- queue payload decoding;
- invoking shared application services;
- transport-level retry/queue integration.

Do not own:

- business logic;
- Task state machine;
- workflow authority;
- direct provider/runtime calls;
- durable job-specific copies of workflow truth.

## 13.2 Replay/idempotency rules

Every workflow-changing job must assume it may be delayed or replayed.

The service invoked by the job must revalidate:

- Task is current;
- exact subject/gate/request is current;
- external side effect has not already completed;
- session binding has not been replaced;
- operation is still authorized.

A retried job must not:

- create replacement Task sessions;
- resend an already known-delivered runtime request;
- create a second canonical PR;
- apply stale acceptance/gate decisions;
- duplicate GitHub mutations.

## 13.3 RQ state

- RQ job existence/status is not workflow truth;
- Postgres owns workflow truth;
- lost jobs are recoverable scheduling failures, not state loss;
- workers should be restartable without reconstructing domain state from memory.

## 13.4 Cross-boundary checks

Queue behavior changes should also read:

- `services/AGENTS.md`;
- relevant adapter guide;
- persistence guidance when delivery/outbox/reconciliation state changes.

---

# 14. `openorc/core/src/openorc/adapters/AGENTS.md`

Adapters are external-system boundaries.

## 14.1 Boundary

Adapters own:

- provider/runtime-native transport;
- authentication mechanics directly consumed by OpenOrc;
- native session/API mechanics;
- harmless syntax/presentation normalization;
- formal response parsing/schema validation where applicable;
- normalized operational/protocol errors;
- authoritative reconciliation helpers supported by the external system.

Adapters do not own:

- OpenOrc Task state transitions;
- Owner authority;
- ReviewLoop meaning;
- generic workflow retry policy;
- durable domain state;
- semantic invention from prose.

## 14.2 Formal-response rule

For v1 agent interactions, the formal contracts are:

- `session_ready`;
- `plan_result`;
- `review_result`;
- `implementation_result`;
- `pr_result`.

Adapters may strip harmless Markdown fences or normalize transport presentation.

They must never “understand what the model probably meant” and manufacture a valid OpenOrc object from unstructured text.

Schema-invalid or semantically invalid formal responses fail through the protocol/error path.

## 14.3 Universal runtime contract

Universal v1 session surface:

```text
create_session(Task, role)
send(session_id, message)
```

Runtime-specific capabilities remain optional/specialized adapter concerns.

Do not widen the universal runtime contract just because Cline exposes more operations.

## 14.4 Authentication boundary

Authentication follows direct consumption:

- OpenOrc stores credentials it directly needs for an OpenOrc-managed Connection;
- runtime-owned model/provider/MCP/tool credentials stay runtime-owned;
- adapters do not turn core into a universal vault.

## 14.5 Fake adapters

Adapter interfaces must be designed so deterministic fake implementations can drive ordinary workflow tests without live GitHub/Cline/model inference.

## 14.6 Cross-boundary checks

Specific GitHub work reads `adapters/github/AGENTS.md`.

Specific Cline work reads `adapters/cline/AGENTS.md` and, when bridge behavior is involved, `packages/cline-sdk-bridge/AGENTS.md`.

---

# 15. `openorc/core/src/openorc/adapters/github/AGENTS.md`

GitHub is a first-class authority boundary.

## 15.1 What GitHub owns

GitHub is authoritative for:

- issues and requirements;
- issue hierarchy/sub-issues/dependencies;
- committed repository code;
- branch/commit/PR state;
- CI/checks/status;
- branch protection;
- merge eligibility/policy;
- merge outcomes.

OpenOrc must not recreate these as competing truths.

## 15.2 Identity and reconciliation

Use stable GitHub identities/IDs for deterministic reconciliation.

Mutable names/URLs/labels/branch names may be stored for presentation/routing but must not be the sole reconciliation key.

Branch name identifies the canonical Task branch; exact commit SHA identifies its current code state.

## 15.3 API vs runtime Git boundary

OpenOrc's GitHub Connection owns workflow-facing GitHub API operations.

Producer runtime owns repository-level Git operations required during implementation:

- create/name feature branch;
- edit files;
- commit;
- push.

After implementation, `implementation_result` reports the Producer-created branch; OpenOrc verifies it against authoritative Git/runtime/GitHub state before binding it.

The Producer does not own canonical PR publication.

## 15.4 Issue semantics

- one current OpenOrc Task per GitHub issue;
- sub-issues define decomposition;
- dependencies define sequencing;
- the review-cleared plan is published as an OpenOrc-authored issue comment;
- do not rewrite the issue body to publish the plan;
- OpenOrc must not treat its own plan comment as changed issue requirements;
- if authoritative issue requirements change after a Task attempt begins, block and require cancel/archive + fresh Task rather than semantic reconciliation.

## 15.5 Webhook boundary

GitHub webhooks:

- are inbound notifications;
- require signature validation;
- require delivery-identity deduplication;
- trigger reconciliation against authoritative GitHub API state;
- are not themselves GitHub truth.

Because GitHub may not automatically redeliver failed webhook deliveries, maintain an independent reconciliation/recovery path.

Out-of-order/replayed/missed notifications must not corrupt workflow state.

## 15.6 PR creation

At `PR_AUTHORIZATION`:

- authorization is bound to exact latest committed Producer head shown to Owner;
- verify canonical branch head immediately before PR creation;
- Producer supplies validated PR title/body through `pr_result`;
- OpenOrc performs actual PR creation/mutation;
- reconcile created PR head immediately afterward;
- if head changed in race window, preserve/record created PR but stop as stale before Reviewer dispatch.

The canonical PR must close its leaf Task issue. Preserve a valid Producer closing reference when present; add one deterministically when absent.

## 15.7 Review and merge

- every canonical PR creation and reconciled PR-head change dispatches formal review against exact head SHA;
- Reviewer does not independently subscribe to GitHub to decide when to review;
- prior acceptance becomes stale when PR head changes;
- base movement alone does not invalidate acceptance;
- GitHub owns conflict/rebase/update/CI/branch-protection rules;
- merge invocation is bound to the exact reviewed or explicitly overridden head SHA using GitHub's expected-head protection where available;
- OpenOrc never fabricates merge success; authoritative GitHub result controls completion.

## 15.8 Required capabilities

The validated v1 fine-grained-PAT baseline includes effective capability equivalent to:

- Issues write;
- Pull requests write;
- Contents write;
- Checks read;
- Commit statuses read;
- Webhooks write.

Repository webhook management requires Owner repository-admin authority.

Equivalent authorization models are acceptable; do not encode PAT as the universal auth model.

## 15.9 Cross-boundary checks

Read:

- `services/AGENTS.md` for workflow timing/authority;
- `domain/AGENTS.md` for Task/PR subject semantics;
- `.github/AGENTS.md` when repository CI/governance itself changes.

---

# 16. `openorc/core/src/openorc/adapters/cline/AGENTS.md`

This file governs the Python Cline adapter and Cline-specific runtime behavior above the language bridge.

## 16.1 Boundary

`ClineAdapter` owns Cline-specific realization of the universal runtime contract and the extra Cline controls OpenOrc actually needs.

It may expose Cline-specific operations such as:

- execution cancellation;
- RuntimeRequest response;
- runtime/session snapshots;
- session telemetry/events;
- execution inspection;
- supported native deep links.

These remain Cline-specific and do not automatically become universal runtime semantics.

## 16.2 Official SDK boundary

OpenOrc v1 uses Cline's official SDK.

Do not:

- implement the Cline Hub wire protocol directly;
- scrape Cline internal files/state when official APIs exist;
- couple domain/services to SDK-native objects;
- expose Node process mechanics above the internal backend contract.

Python depends on a language-neutral `ClineSdkBackend`-style internal boundary.

## 16.3 Session continuity

- one Task/role maps to one persistent external Cline session ID;
- stage changes/retries/reviews do not create sessions;
- bridge/process restart must not imply replacement of external session;
- unexpected session loss blocks;
- `create_session` succeeds only after fresh Task-isolated context is established and valid `session_ready` is received.

## 16.4 Producer/Reviewer isolation

- Producer and Reviewer receive separate sessions and separate mutable working contexts;
- Reviewer stays in Cline PLAN mode for Task lifetime in v1;
- Producer uses Cline PLAN mode during planning;
- Producer changes to ACT only after implementation authorization;
- Producer remains in ACT for implementation and PR remediation and does not return to PLAN after planning is cleared within that Task;
- OpenOrc relies on Cline's native PLAN/ACT permission boundary rather than inventing a second tool-permission system;
- before formal repository review, prepare the existing Reviewer session/worktree against exact committed subject;
- rebuilding/rebinding an internal Cline runtime object under same external session/transcript is allowed where Cline requires it; it is not a new OpenOrc Task session;
- OpenOrc does not recreate or replace Cline's own permission model.

## 16.5 Effective session configuration

- effective runtime/session configuration is captured when the Task role session is created;
- later Workspace role-binding/config changes affect future Task sessions;
- they must not silently mutate an already-bound Task session;
- retain enough effective runtime/session identity for consequential audit, including provider/model identity when Cline reports it.

## 16.6 Provider/model behavior

At the validated v1 baseline:

- Cline owns provider authentication;
- Cline owns provider/model catalogs;
- OpenOrc does not pretend it can remotely select a provider/model when the supported Cline interface cannot do so safely;
- Producer/Reviewer role configuration uses the connected Hub's effective/default provider/model;
- effective runtime/model may be surfaced for audit if available;
- provider/model selection remains stubbed until Cline exposes a supported credential-safe remote catalog/config API.

## 16.7 Runtime ownership

Agent Runtimes are Owner-controlled execution environments.

OpenOrc does not own:

- runtime filesystem;
- repository credentials;
- runtime-local Git auth;
- CLI/MCP/tool credentials;
- provider credentials;
- local dependencies;
- runtime history;
- runtime upgrades;
- execution operations beyond adapter-mediated workflow controls.

A physical runtime may be shared only when its own isolation preserves Workspace boundaries.

## 16.8 Capacity

OpenOrc role binding points to Runtime Connections; it does not create one VM/session binding per Task.

Each Runtime Connection has Owner-configured OpenOrc session capacity, default `3`.

Shared Producer/Reviewer bindings consume the same underlying Connection capacity pool.

Capacity waiting yields `QUEUED`, not `BLOCKED`.

FIFO queue-entry order applies when capacity becomes available.

## 16.9 Delivery semantics

- do not blindly resend an uncertain logical send;
- use SDK/runtime session/history/events to reconcile where possible;
- known delivered/completed dispatch is not resent after job replay;
- known non-delivery may receive bounded retry according to application policy;
- unresolved uncertainty blocks;
- Cline's own within-turn retry/recovery may remain internal after the turn is accepted.

## 16.10 Versioning

Cline CLI and `@cline/sdk` are treated as an explicitly supported semantic-version pair.

Before adopting/upgrading either pin:

- run real adapter/integration compatibility suite;
- verify session lifecycle;
- event/approval behavior;
- remote Hub compatibility;
- review preparation/worktree behavior;
- relevant protocol expectations.

Do not pin arbitrary `main` commits for production support when official releases exist.

## 16.11 Cross-boundary checks

Read:

- `adapters/AGENTS.md`;
- `services/AGENTS.md`;
- `domain/AGENTS.md`;
- `packages/cline-sdk-bridge/AGENTS.md` for Node/stdio mechanics;
- Cloud runtime deployment guidance when changing reference Hub configuration.

---

# 17. `openorc/core/src/openorc/persistence/AGENTS.md`

Persistence implements durable OpenOrc workflow/control truth.

## 17.1 Boundary

Own:

- repositories/data access;
- durable representation of domain state;
- transaction boundaries;
- persistence constraints;
- mapping between domain and Supabase/Postgres structures.

Do not let storage schema invent product semantics accidentally.

## 17.2 Core rules

- every Workspace-owned record must be explicitly Workspace-scoped;
- enforce one current/non-archived Task per GitHub issue at the data/domain boundary;
- preserve stable external identity keys needed for reconciliation;
- exact subject references used for gates/reviews/executions must be durable;
- workflow state, not queue jobs/transcripts, is durable truth;
- do not persist pretend provider/model selections unsupported by runtime;
- secrets are references/status where possible, not ordinary raw application fields;
- transcript/high-frequency telemetry is not persisted merely because it exists;
- `WorkflowEvent` durability is for independent audit/workflow meaning, not a copy of every runtime event.

## 17.3 Transactions and concurrency

Persistence/application operations must support stale-operation protection.

Where state transition correctness depends on current subject/version:

- use transaction/constraint/compare-and-set style protection appropriate to implementation;
- do not rely only on in-memory checks;
- duplicate/replayed operations must be safe.

Exact table/outbox implementation remains an implementation choice unless later settled.

## 17.4 Cross-boundary checks

Schema/model changes require:

- `domain/AGENTS.md`;
- `services/AGENTS.md`;
- `supabase/AGENTS.md`;
- API/frontend guides when public contracts change.

---

# 18. `openorc/core/supabase/AGENTS.md`

Supabase is the supported v1 persistence/auth implementation in core.

## 18.1 Boundary

Core owns:

- schema/migrations;
- Supabase persistence integration;
- Supabase Auth integration required by product;
- least-privilege/RLS policy definitions;
- generated artifacts if adopted.

Core does **not** own the concrete OpenOrc Cloud production Supabase project or its operations.

## 18.2 Migration rules

Carry forward the safe generic Cline defaults:

- apply migrations through configured Supabase MCP during active development when appropriate;
- every applied product migration must also exist in repository migration files;
- repository migration content and applied DDL must remain aligned;
- public-schema changes explicitly define least-privilege grants;
- do not rely on broad default `anon`, `authenticated`, or `service_role` privileges;
- do not hardcode generated environment/project identifiers in product migrations;
- project ID comes from actual environment/repo configuration, never guessed from Babelbeez.

## 18.3 RLS and Workspace isolation

- Workspace isolation is a security boundary;
- RLS/authorization must preserve explicit Workspace/user ownership;
- service-role/server credentials remain server-side;
- browser clients never receive server/service credentials;
- auth/storage design must not make one Workspace able to infer or access another Workspace's state.

## 18.4 Generated types

If generated Supabase types are adopted:

- generated files are regenerated, not hand-maintained;
- schema changes regenerate them in same task;
- consumers affected by type/contract changes must be validated.

Do not copy Babelbeez-specific project IDs or migration-history accommodations.

## 18.5 Cross-boundary checks

Read persistence, domain/services, and frontend/API guidance when schema/RPC/type changes cross those boundaries.

---

# 19. `openorc/core/packages/cline-sdk-bridge/AGENTS.md`

This file is mandatory from day one because the bridge boundary is unusually easy to corrupt.

## 19.1 Boundary

The package is a thin Node/TypeScript language compatibility shim:

```text
Python ClineAdapter
↓
internal backend contract
↓
long-lived local Node subprocess
↓
JSON-RPC 2.0 over stdin/stdout
↓
official @cline/sdk
↓
remote Cline Hub
```

## 19.2 It owns only

- official SDK client invocation;
- request/reply correlation;
- plain-JSON translation;
- SDK connection lifecycle;
- asynchronous event forwarding;
- approvals/usage/runtime state forwarding supported by SDK;
- bridge-local protocol/version compatibility.

## 19.3 It must never own

- Tasks;
- PlanRevisions;
- ReviewLoops;
- OwnerGates;
- Executions;
- RuntimeRequests as workflow concepts;
- OpenOrc retry/idempotency policy;
- formal OpenOrc response schema validation;
- GitHub operations;
- authorization decisions;
- durable database state;
- workflow transitions;
- independent network service endpoint;
- service discovery/scaling identity.

## 19.4 Process lifecycle

- bridge is started/lifecycle-managed locally by Python;
- no separate deployment/service is created for it;
- bridge restart/failure does not imply Task session replacement;
- persistent external Cline session identity remains above/beyond bridge process lifetime;
- bridge must remain replaceable by future native Python Cline SDK implementation without changes above backend boundary.

## 19.5 Versioning/testing

- pin official released `@cline/sdk` semantic version;
- keep bridge protocol explicit/versioned enough to test compatibility;
- contract tests cover request/reply correlation and async events;
- real compatibility suite covers supported remote Hub/CLI + SDK pair before upgrade.

---

# 20. `openorc/core/apps/app/AGENTS.md`

The product SPA is an Owner control surface, not a second workflow engine.

## 20.1 Boundary

`apps/app` is the authenticated/product Vue SPA. The name `web` is reserved for a distinct public/marketing website if OpenOrc later grows one.

Own:

- Vue 3 / Vite / TypeScript application;
- Owner-facing views;
- local UI state;
- server-state queries/mutations;
- authorized SSE consumption;
- presentation/navigation to authoritative external systems.

Do not own:

- workflow transitions independently of backend;
- direct runtime calls;
- direct GitHub workflow mutations bypassing OpenOrc;
- hidden provider control planes;
- server-derived authority state in client-only stores.

## 20.2 Framework baseline

Starting standards:

- Vue 3 Composition API with `<script setup lang="ts">`;
- TypeScript strictness;
- PrimeVue for component system;
- Tailwind for layout/spacing where adopted by bootstrap;
- TanStack Query for server-derived state, fetching, caching, mutation invalidation;
- Pinia only for genuine client/application state that is not server truth.

Exact visual theme/component composition can evolve without rewriting architectural rules.

## 20.3 API/state rules

- components/views do not scatter raw `fetch` calls when API client/query layers exist;
- components/views do not call Supabase tables directly for OpenOrc workflow data;
- OpenOrc API is the workflow/application authority;
- SSE provides live updates but server APIs/durable state remain authoritative;
- runtime telemetry is display data and may legitimately be `UNKNOWN`;
- runtime session capacity is separate from per-Task session traffic-light health.

## 20.4 Task UI invariants

- main dashboard is primary happy path;
- every active Task wizard occupies its own tab;
- never introduce a global singleton “current Task” assumption;
- Owner Requests surface durable gates/runtime approvals;
- Reviewer discussion is Owner ↔ Reviewer only;
- no UI path creates free-form Owner ↔ Producer chat;
- merge/implementation authorization are explicit user actions tied to exact current subject;
- stale/blocked recovery state must be durable and visible.

## 20.5 External surfaces

OpenOrc should link to GitHub/Cline/provider native interfaces for deeper inspection rather than reimplementing them unnecessarily.

External links are navigation only, never hidden application-data/control transports.

## 20.6 Errors

- centralized transient API errors should use the shared app error/toast pipeline;
- avoid duplicate generic local error toasts;
- durable workflow failures/blocks belong in Task/Owner Request surfaces, not ephemeral-only toasts.

## 20.7 Mobile/PWA

- UI should be mobile-friendly;
- PWA posture is intended;
- do not invent offline workflow authority or client-side execution semantics.

## 20.8 Verification

The current Owner development preference against ad-hoc browser automation belongs in workspace-level `openorc-rules.md`, not as an eternal frontend architecture invariant.

Repository tests should still include frontend component/state/API-contract coverage appropriate to v1.

---

# 21. `openorc/core/tests/AGENTS.md`

A dedicated test guide is justified because `tests/` sits outside the source subtrees and therefore would not automatically inherit their local context.

## 21.1 Context traversal

Before editing a test, read the `AGENTS.md` for the subsystem under test.

Examples:

```text
domain/service test
→ read domain/services guidance

GitHub adapter test
→ read adapters + github adapter guidance

Cline bridge test
→ read Cline adapter + bridge guidance

frontend test
→ read apps/app guidance
```

## 21.2 Ordinary test philosophy

Tests should be:

- behavior-focused;
- deterministic;
- independent of live infrastructure for normal runs;
- explicit about authority/staleness/isolation boundaries;
- designed around fake adapters and controlled providers.

Do not chase line coverage for its own sake.

## 21.3 Required categories

The grounding document expects coverage across:

- frontend component/state;
- API contracts;
- domain/services;
- workflow transitions;
- persistence;
- RQ jobs;
- adapter contracts;
- formal response protocol;
- Reviewer contract;
- Task session lifecycle/isolation;
- prompt/default/override/protocol envelope;
- communication topology;
- GitHub adapter/reconciliation.

## 21.4 High-risk invariants tests must preserve

Include tests proving, where applicable:

- fresh isolated sessions per Task/role;
- no session replacement on replay/stage transition;
- Reviewer exact committed subject;
- runtime capacity queueing/FIFO;
- cross-Task working-context isolation;
- malformed formal responses are rejected, not inferred;
- known delivered dispatch is not replayed;
- uncertain external outcome blocks;
- stale jobs/actions cannot mutate newer subject;
- GitHub webhook dedupe + authoritative reconciliation;
- PR-head changes invalidate exact-head acceptance;
- base movement alone does not;
- PR creation race handling;
- merge expected-head protection;
- cancellation does not mutate unrelated GitHub state;
- UI does not become independent workflow authority;
- Workspace isolation.

## 21.5 Live integration/E2E

Normal test suites must not silently call production/live GitHub, Supabase, Valkey, Cline Hub, or model inference.

Real integration/E2E environments are explicit test layers/issues.

The real headless E2E phase should prove the actual external architecture before the UI is built around assumptions.

## 21.6 `tests/README.md`

A long `tests/README.md` is entirely compatible with a concise `tests/AGENTS.md`.

Keep explanatory material such as commands, fixture layout, marker strategy, local integration setup, suite organization, examples, and troubleshooting in `tests/README.md`.

Keep only durable operating constraints in `tests/AGENTS.md`. The two files are complementary, not alternatives.

---

# 22. `openorc/core/.github/AGENTS.md`

## 22.1 Boundary

Own:

- CI;
- repository automation;
- issue/PR templates if used;
- release automation if adopted;
- dependency automation;
- workflow permissions;
- repository workflow names coupled to protection rules.

Core `.github` does not own OpenOrc Cloud production deployment.

## 22.2 CI/governance rules

- normal changes use branch + PR;
- protected `main` rules should be reflected here once configured;
- keep required workflow/job names stable after branch protection depends on them;
- workflows never embed real secrets;
- least-privilege GitHub Actions permissions;
- CI runs repository-defined quality gates;
- quality gates should be greenfield-clean rather than copying Babelbeez's changed-files-only legacy-debt policy;
- actual label taxonomy belongs here only after one is adopted;
- do not invent release/deploy labels merely because Babelbeez had them.

## 22.3 Merge/update policy

Document the policy actually configured for `openorc/core`.

Do not inherit a merge strategy from Babelbeez workspace rules.

GitHub is authoritative for merge eligibility.

## 22.4 Cross-boundary checks

CI changes touching tests/tooling read `tests/AGENTS.md`.

Any future package/release process reads the relevant package guidance.

---

# 23. Recommended `openorc/cloud` Repository Shape

Bootstrap:

```text
openorc/cloud/
├── .github/
│   ├── AGENTS.md
│   └── workflows/
│
├── src/
│   └── openorc_cloud/
│       ├── AGENTS.md
│       └── billing/
│           └── AGENTS.md
│
├── deployment/
│   ├── AGENTS.md
│   ├── README.md
│   ├── do-app.yaml
│   └── cline/
│       ├── AGENTS.md
│       ├── producer/
│       └── reviewer/
│
├── operations/
├── docs/
├── AGENTS.md
├── README.md
└── .gitignore
```

`operations/AGENTS.md` should be added only after operational runbooks/tools actually exist and acquire durable local constraints.

A separate `billing/polar/AGENTS.md` is unnecessary initially. `billing/AGENTS.md` can own the hosted billing boundary until the subtree becomes large enough to justify another perimeter.

---

# 24. `openorc/cloud/AGENTS.md`

## 24.1 Repository identity

State explicitly:

- Cloud is private hosted/reference composition around the complete core product;
- dependency direction is `cloud → core`;
- core never imports Cloud;
- Cloud must not fork or reimplement core workflow/domain semantics;
- Cloud is the first dogfood/reference deployment, not a different product.

## 24.2 What Cloud owns

- concrete hosted control-plane infrastructure;
- concrete production Supabase project/operations;
- concrete managed Valkey connection/deployment;
- GitHub production Environment values/secrets;
- hosted monitoring/recovery/backup operations;
- reference/dogfood Cline runtime infrastructure;
- hosted account/billing extension;
- Polar integration.

## 24.3 What Cloud does not own

- universal OpenOrc workflow;
- core Task/session/review state machine;
- universal Agent Runtime contract;
- core GitHub adapter semantics;
- self-hosted feature gating;
- user runtime/provider credentials;
- customer runtime filesystem/operations.

## 24.4 BYO runtime boundary

OpenOrc Cloud v1 connects to Owner-controlled Agent Runtimes.

It does not operate a shared/per-customer Cline Hub as part of the base hosted service.

Reference/dogfood runtime deployment exists to operate our own first deployment and demonstrate a hardened topology, not to redefine core product ownership.

## 24.5 Billing boundary

- Polar is Cloud-only;
- one OpenOrc Cloud account/customer subscription;
- billing quantity = active Workspace count;
- one fixed Workspace unit price;
- no seat/token/model/execution/feature-tier dimensions;
- archived Workspaces are non-billable;
- billing may gate hosted Workspace creation/activation;
- billing must never weaken Workspace isolation or alter core workflow semantics;
- exact currency/unit price are configuration, not domain semantics.

## 24.6 Context perimeter map

Read:

- `src/openorc_cloud/AGENTS.md`;
- `src/openorc_cloud/billing/AGENTS.md`;
- `deployment/AGENTS.md`;
- `deployment/cline/AGENTS.md`;
- `.github/AGENTS.md`.

---

# 25. `openorc/cloud/src/openorc_cloud/AGENTS.md`

## 25.1 Boundary

This package is the hosted-extension seam.

It may compose or extend core at explicit supported seams.

It must not:

- copy core workflow state machine;
- fork core services;
- redefine Task/session semantics;
- make core depend on Cloud imports;
- make self-hosting incomplete.

## 25.2 Hosted account/entitlement seam

Cloud-specific account/billing behavior should be kept behind the smallest explicit integration boundary necessary for the shared application.

Commercial policy must remain orthogonal to engineering workflow semantics.

## 25.3 Testing

Hosted extensions should be independently testable with core interfaces/fakes.

Do not require live production Polar or DigitalOcean for ordinary unit/integration tests.

---

# 26. `openorc/cloud/src/openorc_cloud/billing/AGENTS.md`

## 26.1 Boundary

Own hosted billing/account synchronization with Polar.

Do not own core workflow or Workspace isolation semantics.

## 26.2 Billing model

Canonical commercial model:

```text
one hosted billing customer/subscription
× active Workspace quantity
× fixed Workspace unit price
```

Do not introduce:

- seats;
- token usage;
- model usage;
- execution volume;
- feature tiers;
- permanent free tier

without an explicit product-design change.

## 26.3 Authority

Polar owns payment/subscription facts that Cloud consumes.

OpenOrc Cloud owns its hosted account/Workspace activation consequences.

Billing synchronization must be deterministic/idempotent and must not grant access across Workspace boundaries.

Exact webhook/RPC mechanics remain implementation detail until built.

## 26.4 Cross-boundary checks

Read Cloud root and core Workspace/auth boundaries when billing changes affect hosted activation.

---

# 27. `openorc/cloud/deployment/AGENTS.md`

## 27.1 Boundary

Own the concrete OpenOrc Cloud DigitalOcean deployment model.

## 27.2 Control-plane topology

The intended control plane is one DigitalOcean App Platform app containing:

```text
Vue static SPA
FastAPI service
RQ worker
```

The Cline SDK bridge remains a local child process of control-plane processes that need it; it is not a separate service.

Producer/Reviewer Cline Hubs are outside this App Platform control plane.

## 27.3 Configuration ownership

- tracked deployment YAML owns desired-state structure;
- production values are individually owned by the GitHub `production` Environment as variables/secrets;
- do not recreate Babelbeez `.env.production.*` mirror blobs;
- do not introduce Babelbeez-style custom `render_do_specs.py` configuration maps;
- do not create persistent generated deployment specs as a second source of truth;
- if substitution is required, materialize deploy-ready specs transiently in workflow-runner temporary storage.

## 27.4 Hosted infrastructure ownership

Cloud deployment owns concrete:

- DigitalOcean App Platform app;
- production Supabase project wiring;
- managed Valkey wiring;
- networking;
- runtime Connection/reference Cline VM deployment;
- operational environment configuration.

Core owns the behavior/code that consumes these systems.

## 27.5 Rebuildability

Deployment must be reproducible from:

```text
repository desired state
+
GitHub Environment configuration
+
documented external resource prerequisites
```

No critical production-only state should live solely in a developer laptop file.

## 27.6 Cross-boundary checks

Read:

- Cloud `.github/AGENTS.md` for deployment workflow changes;
- `deployment/cline/AGENTS.md` for runtime infrastructure;
- core runtime/persistence boundaries when infrastructure changes affect product assumptions.

---

# 28. `openorc/cloud/deployment/cline/AGENTS.md`

This guide separates reference runtime operations from OpenOrc core semantics.

## 28.1 Boundary

Own reference/dogfood Cline Hub deployment/configuration.

It does not define the universal Agent Runtime contract.

## 28.2 Reference topology

Current validated reference posture:

- isolated VPS/VM per runtime Connection;
- Producer and Reviewer may therefore run on separate Hubs in the first deployment;
- architecture still permits one sufficiently capable Workspace-specific Hub to host isolated Producer and Reviewer sessions in the future;
- separate role runtimes remain a valid hardened topology;
- one VPS per Task is **not** the operating model.

## 28.3 Runtime ownership

The runtime VM owns:

- Cline CLI;
- provider credentials;
- Git credentials;
- MCP/tool credentials;
- repository checkout/worktrees;
- runtime-local dependencies/config;
- Cline session history;
- upgrades/operations.

OpenOrc Cloud stores only the Connection/configuration needed to orchestrate it.

## 28.4 Version pinning

- pin official Cline CLI semantic version;
- coordinate it with core's pinned `@cline/sdk` supported pair;
- upgrades require compatibility validation before adoption;
- do not follow Cline `main` implicitly.

## 28.5 Cline Hub global rules

This is where a **reference runtime Cline baseline** may eventually be provisioned.

Do not copy the build-time Owner `default-rules.md` verbatim.

A future runtime baseline may safely include concepts such as:

- inherited repository `AGENTS.md` loading;
- cross-boundary context traversal;
- patch/tool safety that still applies to Hub execution;
- generic test discipline;
- safe documentation/tool lookup;
- generic database least-privilege guidance where relevant.

It must not include manual-development behavior that conflicts with OpenOrc orchestration, such as:

- Cline autonomously opening the canonical PR;
- Cline deciding merge progression;
- hard-coded merge method;
- automatic repository-update policy;
- free-form user questioning outside OpenOrc protocol;
- reliance on `attempt_completion` semantics from a different harness.

Role-specific Producer/Reviewer behavior belongs in OpenOrc initialization/prompts, not Hub-global config.

## 28.6 Security/isolation

- runtime provider/tool/Git credentials remain on Owner-controlled runtime;
- OpenOrc must not ingest them merely for convenience;
- physical runtime sharing across Workspaces is allowed only when runtime isolation is actually sufficient;
- directory/workspace separation alone must not be assumed to be a security boundary.

---

# 29. `openorc/cloud/.github/AGENTS.md`

## 29.1 Boundary

Own Cloud CI/deployment automation and GitHub Environment interaction.

## 29.2 Production environment

- production variables/secrets live in GitHub `production` Environment;
- workflows reference individual values;
- no real secrets in repository YAML;
- deployment workflows should preserve approval/environment protections configured by GitHub;
- least-privilege workflow permissions.

## 29.3 Core dependency

Cloud consumes core.

Dependency/version/update mechanics must not create a reverse core→Cloud dependency or hidden local-only linkage.

## 29.4 Deployment

Deployment workflow changes read `deployment/AGENTS.md`.

Runtime provisioning workflow changes also read `deployment/cline/AGENTS.md`.

Do not import Babelbeez release-label taxonomy unless OpenOrc Cloud deliberately adopts an equivalent later.

---

# 30. Cross-Repository Context Rules

Some work necessarily spans both repositories.

Examples:

```text
core hosted-extension seam change
↔ Cloud extension implementation

core deployment requirements
↔ Cloud concrete deployment

core Cline adapter supported version pair
↔ Cloud runtime CLI pin

core schema migration
↔ Cloud production Supabase operation

core configuration requirement
↔ Cloud GitHub Environment/deploy wiring
```

For cross-repository changes:

- core remains the dependency owner for product behavior;
- Cloud adapts/conforms;
- do not solve Cloud-specific needs by leaking concrete Cloud assumptions into core;
- if core introduces a new explicit hosted extension seam, document the seam in core root/local guidance;
- implementation issues should link dependencies across repositories where sequencing matters.

---

# 31. What Must Not Be Copied from Babelbeez

Do not bootstrap OpenOrc with:

- Babelbeez hostnames;
- Babelbeez Supabase project ID;
- voice-agent/session endpoints;
- Firecrawl/RAG rules;
- n8n workflow rules;
- Make/WordPress integration rules;
- Paddle rollback machinery;
- Babelbeez minute accounting;
- Redis DB `0` / DB `1` split;
- Babelbeez queue names;
- Make organization/team/connection IDs;
- Babelbeez SDK/embed rules;
- public-site SEO rules;
- Babelbeez release tag/label taxonomy;
- historical issue numbers/PR numbers;
- changed-files-only static checking used to accommodate Babelbeez legacy debt;
- Babelbeez `.env.production.*` mirroring;
- `render_do_specs.py`;
- persistent generated deployment specs;
- assumptions that the user manually reviews every frontend change forever.

OpenOrc should inherit the lessons, not the scar tissue.

---

# 32. Bootstrap Deliverables

When the GitHub connector is moved to `openorc` and the two repositories are ready, bootstrap should proceed in roughly this order.

## 32.0 OpenOrc VS Code / Cline workspace

Create:

```text
openorc-rules.md
```

This file owns development-Cline workflow/tooling behavior for work across the OpenOrc repositories.

It should be adapted from Appendix A rather than copied blindly from Babelbeez. In particular:

- keep context-loading and cross-boundary traversal behavior;
- keep safe task-branch/PR hygiene appropriate to the Owner's development workflow;
- allow task-scoped maintenance of relevant `AGENTS.md` and `README.md` without requiring a separate Owner confirmation;
- keep testing discipline;
- keep patch/tool safety that applies to the current Cline harness;
- keep documentation-fetch/tool priorities that actually exist in the OpenOrc workspace;
- keep generic Supabase least-privilege migration discipline where useful;
- remove Babelbeez-only merge/update assumptions unless deliberately adopted for OpenOrc;
- do not put OpenOrc product Producer/Reviewer protocol semantics here;
- do not make repository portability depend on this file.

`openorc-rules.md` complements the repositories' `AGENTS.md` cascades; it does not replace them.

## 32.1 `openorc/core`

Create/establish:

```text
LICENSE (Apache-2.0)
README.md
.gitignore
AGENTS.md

.github/AGENTS.md
apps/app/AGENTS.md
src/openorc/AGENTS.md
src/openorc/domain/AGENTS.md
src/openorc/services/AGENTS.md
src/openorc/api/AGENTS.md
src/openorc/workers/AGENTS.md
src/openorc/adapters/AGENTS.md
src/openorc/adapters/github/AGENTS.md
src/openorc/adapters/cline/AGENTS.md
src/openorc/persistence/AGENTS.md
packages/cline-sdk-bridge/AGENTS.md
supabase/AGENTS.md
tests/AGENTS.md
tests/README.md
```

Create the structural directories from the grounding document even before they contain full implementations so Cline begins from the intended architecture rather than a generic frontend/backend layout.

Add `activeTask.md` to `.gitignore`.

## 32.2 `openorc/cloud`

Create/establish:

```text
README.md
.gitignore
AGENTS.md

.github/AGENTS.md
deployment/AGENTS.md
deployment/cline/AGENTS.md
src/openorc_cloud/AGENTS.md
src/openorc_cloud/billing/AGENTS.md
```

Create initial structural deployment directories:

```text
deployment/cline/producer/
deployment/cline/reviewer/
operations/
docs/
```

Do not create fake production configuration values merely to fill the tree.

## 32.3 Grounding document handling

At bootstrap time, preserve a repository-accessible copy of the current grounding document if it is still serving as the early canonical design source.

Do **not** require Cline to ingest the full grounding document on every issue.

Root/nested `AGENTS.md` plus issue context are the normal operating context.

The grounding document remains an escalation/reference source for architecture questions not yet compiled into repository-native truth.

## 32.4 Initial GitHub governance

Configure repository governance separately from Cline defaults.

At minimum establish:

- protected `main`;
- branch + PR development flow;
- initial required CI checks once workflows exist;
- human merge authority;
- actual allowed merge method(s);
- actual branch-update requirement;
- repository webhook/app permissions as needed.

Then document the **actual** policy in `.github/AGENTS.md` and root, rather than assuming Babelbeez settings.

---

# 33. Bootstrap Review Checklist

Before implementation issue decomposition begins, verify:

### Context architecture

- root guide is concise;
- every nested guide has a genuine local purpose;
- cross-boundary links are present;
- no guide copies the grounding document wholesale;
- no guide contains Babelbeez IDs/hostnames/history;
- `activeTask.md` is local/untracked.

### Core/Cloud separation

- core is complete without Cloud;
- Cloud imports/composes core, never the reverse;
- Supabase product schema/integration is core;
- concrete production Supabase is Cloud;
- RQ behavior is core;
- concrete Valkey is Cloud;
- Cline adapter/SDK bridge is core;
- concrete reference runtime deployment is Cloud;
- Polar is Cloud-only.

### Backend architecture

- routers are transport-only;
- jobs are queue transport-only;
- services own use-case orchestration;
- domain owns semantics/invariants;
- adapters own external mechanics;
- persistence owns durable representation;
- no workflow state in RQ/transcripts.

### Cline boundary

- official SDK only;
- Node bridge has no workflow logic;
- persistent session identity survives bridge process lifecycle;
- CLI + SDK semantic releases are explicitly paired;
- workspace-level `openorc-rules.md` is not blindly used as runtime Product Cline policy.

### GitHub boundary

- webhooks are notifications + reconciliation;
- GitHub remains authority for engineering record;
- Producer owns branch/commit/push;
- OpenOrc owns canonical PR publication;
- review is exact-head addressed;
- merge policy remains GitHub/repo-owned.

### Testing

- fake adapters are first-class;
- ordinary tests need no live infrastructure;
- `tests/AGENTS.md` routes test authors back to source subsystem guidance;
- behavior changes require tests;
- regression fixes require regression tests;
- real headless E2E is explicit later phase.

### Deployment

- one DO App Platform control-plane app;
- Cline Hubs outside the control-plane app;
- no `.env.production.*` mirror model;
- no custom persistent generated-spec layer;
- production values in GitHub Environment;
- deploy desired state remains reconstructable.

---

# 34. When to Split the Cascade Further

Do not predict the entire future directory tree.

Add new local `AGENTS.md` files when implementation creates durable local constraints, for example:

```text
src/openorc/services/<large-domain>/AGENTS.md
src/openorc/adapters/<future-runtime>/AGENTS.md
operations/AGENTS.md
src/openorc_cloud/billing/polar/AGENTS.md
```

A split is justified when the parent guide would otherwise begin accumulating:

- provider-specific rules;
- protocol quirks;
- unique security boundaries;
- subsystem-specific commands/workflows;
- detailed invariants irrelevant to sibling code.

The test is simple:

> Would an agent editing sibling code be burdened or misled by these rules?

If yes, move them into a deeper perimeter.

---

# 35. End-State Principle

The desired result is not “a lot of `AGENTS.md` files.”

The desired result is:

```text
Cline enters the OpenOrc workspace
↓
loads workspace-level openorc-rules.md
↓
opens a GitHub issue
↓
loads root repository context
↓
loads only the relevant local perimeters
↓
understands what each layer owns
↓
knows which shortcuts are forbidden
↓
implements inside the established architecture
↓
tests the relevant invariants
↓
submits a PR without needing the Owner or ChatGPT to restate the architecture
```

The grounding document gave us unusually deep design certainty before code exists. The repository context should exploit that advantage without reproducing the grounding document verbatim.

The cascade should make the correct architecture the easiest path.

---

# Appendix A — Source Rule Set for `openorc-rules.md`

The following preserves the rule content originally supplied from the now-retired global `default-rules.md`, with chat/Markdown escaping normalized for readability.

Its role in this bootstrap specification is now:

```text
source material
→ adapt for OpenOrc
→ save as workspace-level openorc-rules.md
```

It is **not** a repository `AGENTS.md`, and it is **not** a product-runtime Cline Hub specification.

When adapting this source into `openorc-rules.md`, intentionally remove the old requirement to ask the user before updating `AGENTS.md`. That behavior suited the current manual loop but conflicts with the future OpenOrc Producer topology, where no free-form Owner ↔ Producer channel exists. Relevant `AGENTS.md` and `README.md` maintenance should instead occur inside the task branch/PR and be reviewed normally.

```markdown
# Cline Agent Rules

You are Cline. Follow these rules for every task, in every repository. Repo-specific context belongs in that repo's `AGENTS.md` files — these global rules are intentionally repo-agnostic.

## Context Loading

Use inherited `AGENTS.md` files as the active operating context.

- Start from the root repository context.
- Before editing files, read the nearest applicable subsystem `AGENTS.md` for the files or directories involved.
- If a task spans multiple subsystems, read every relevant subsystem `AGENTS.md` before editing.
- Prefer localized subsystem rules over broad assumptions.

## Active Task Context

Use root-level `activeTask.md` for persistent local task context when needed.

- `activeTask.md` is local working context and should always be read first.
- Use it for the current task focus, TODOs, implementation notes, decisions, and next steps.
- Do not treat `activeTask.md` as repo-portable architecture documentation.
- Ask the user before creating or updating `activeTask.md` unless they explicitly request it.

## Cross-Boundary Context Traversal

When work crosses runtime, network, API, deployment, database, integration, or frontend/backend boundaries, read all relevant subsystem rules before editing. Treat the repo's root `AGENTS.md` as the map that identifies its subsystem perimeters.

## AGENTS.md Maintenance

After completing a task, consider whether any `AGENTS.md` file should be updated.

- Update or propose updates when a durable rule, boundary, workflow, command, security constraint, deployment expectation, or integration behavior has changed.
- Do not update `AGENTS.md` for one-off implementation details, temporary debugging notes, or task-specific TODOs.
- Ask the user before updating `AGENTS.md` unless the update was explicitly requested.

## Repository Workflow

Repository changes should be made on task branches and submitted through PRs to protected `main`; do not plan direct commits to `main` as the normal workflow. Expect PR review, resolved conversations, up-to-date branches, and CI checks before merge. Do not wait for or poll the CI check - user will monitor the PR and inform you if there is a problem. In the PR description, note which deployment surfaces or servers are affected.

If the repository defines a PR label convention that drives automation (e.g. per-surface release notes), apply the correct labels at PR creation time based on the files touched; multi-surface changes get every label that applies. Read the repo's `AGENTS.md` files for the concrete label taxonomy. Never leave labels blank in a repo where labels drive automation — an uncategorized PR degrades the automation's output rather than breaking it.

PRs merge via merge commit; agents never merge PRs — the user merges in GitHub. When a PR falls behind `main` (repos with strict up-to-date status checks require CI to run on the latest base), rebase the task branch onto latest `origin/main`, push with `git push --force-with-lease`, and let CI re-run before handing the PR to the user to merge.

Before creating a new task branch for repo changes:

- if the current branch's PR has already merged (verify with `gh pr view <branch> --json state`, or user confirmation), prune it first: `git fetch --prune`; switch to `main`; fast-forward; `git branch -d <merged-branch>`;
- verify a clean working tree;
- fetch `origin`;
- switch to `main`;
- fast-forward local `main` from `origin/main`;
- create the task branch from updated `main`.

For already-in-progress branches, inspect the branch first and then merge or rebase latest `main` deliberately.

New features and behavior changes require tests in the same PR; bug fixes require a regression test. Follow the repo's documented testing baseline where one exists.

## Frontend UI Verification

Do not run visual browser checks (e.g. Puppeteer/browser automation) to validate frontend UI changes, and do not start ad hoc local servers to spot-check pages. The user reviews UI manually after the task is complete.

Limit frontend verification to builds, static artifact checks (dist output, sitemap, asset paths), and automated tests.

## Task Completion

When calling `attempt_completion`, do not provide the `command` parameter. Attached commands may execute without explicit user approval; present the result summary only and let the user run any review or verification commands themselves.

## Patch Editing Safety

When rewriting an existing file with `apply_patch`, use `*** Update File`, not a delete-and-add pair for the same path.

- Use `*** Add File` only for files that do not already exist.
- Use `*** Delete File` only when intentionally removing a file.
- Do not combine `*** Delete File` and `*** Add File` for the same path in one patch.
- For full-file rewrites, prefer one `*** Update File` patch that replaces the existing content.

## Supabase

Apply database migrations directly using the Supabase MCP tool when one is configured.

- Get the project ID from the repo's own `AGENTS.md` or configuration — never assume or hardcode one.
- Never add `task_progress` to SQL arguments.
- Public-schema migrations must explicitly define least-privilege grants for any new or changed tables, sequences, views, or routines.
- Do not rely on broad default privileges for `anon`, `authenticated`, or `service_role`.

## Web Fetching

Fetch web content with the cheapest tool that answers the question, in this priority order:

1. **Specialized documentation MCP servers first when they cover the target.** If an MCP server provides direct access to the vendor's own documentation (e.g. OpenAI developer docs, PrimeVue, Supabase doc search), prefer it — it indexes the canonical source directly and beats any general search.
2. **Developer Index next for library/API/framework questions** (behavior, error messages, API contracts, known bugs) not covered by a specialized doc server: use Firecrawl's developer search (`firecrawl_search` with `categories: ["developer"]`, the dedicated developer-search tool, or the Firecrawl CLI developer index). Its matched passages usually answer the question directly and identify the canonical URL — no second fetch needed. Use `skills: "only"` when looking for agent-skill guidance.
3. **Plain fetch for known URLs and general web questions.** Use the built-in web fetch/search when you already have the URL, when the question is not developer-focused, or when a Developer Index result's URL needs full-page content. If result is truncated use Firecrawl scrape.
4. **Firecrawl scrape/crawl only when a plain fetch cannot get the content** — JS-rendered pages, PDFs, structured extraction, multi-page crawls, or `firecrawl_map` to discover URLs on a site. Never as a substitute for steps 1 or 2.
```

---

# Appendix B — Candidate Future Runtime-Hub Baseline

This is **not yet an adopted configuration**. It records the safe direction for later reference Cline Hub provisioning.

A runtime-Hub baseline should remain small and repo-agnostic. It is separate from the development workspace's `openorc-rules.md`:

```text
1. Load inherited AGENTS.md and nearest subsystem context.
2. Read every relevant subsystem guide for cross-boundary work.
3. Treat repository-local rules as authoritative for repo policy.
4. Do not weaken tests/security merely to get a task green.
5. New behavior requires tests; bug fixes require regression coverage.
6. Never commit secrets or leak credentials in output/logs.
7. Prefer official/canonical documentation and supported APIs.
8. Use safe patch/edit operations appropriate to the actual harness.
9. Follow least-privilege database migration rules.
10. Maintain relevant AGENTS.md/README.md when a task changes durable repository context; do not require a free-form Owner confirmation.
11. Do not invent repository merge/update/label policy.
```

OpenOrc Producer/Reviewer role behavior, formal output schemas, PR ownership, review semantics, and authority gates remain OpenOrc initialization/protocol concerns and must not be hidden inside runtime-global Cline rules.
