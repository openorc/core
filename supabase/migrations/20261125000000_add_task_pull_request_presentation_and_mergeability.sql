-- TaskPullRequest presentation projection extension and head-bound
-- mergeability observation (Phase 2G, issue #376).
--
-- GitHub remains the engineering source of truth; the canonical TaskPullRequest
-- record deliberately retains selected typed facts, never the raw provider
-- response. Every new column is nullable: an existing row has not yet observed
-- the fact, and the record stays honest until a successful reconciliation
-- supplies it. No default fabricates a provider fact; migrations preserve
-- existing PR identity, Task links, and workflow state untouched. There are no
-- workflow consequences: observation is presentation evidence, never merge
-- authority, and GitHub remains the final merge authority.
--
-- - `title`, `body` (GitHub's documented nullable body verbatim), `html_url`
--   (the verbatim provider navigation fact, never reconstructed), the bounded
--   `user_id`/`user_login` subset (a deleted account reports a null user), and
--   `draft` are the agreed native presentation facts.
-- - `provider_created_at`/`provider_updated_at`/`provider_closed_at` are the
--   provider-reported lifecycle instants, named distinctly from the record's
--   own `created_at`/`updated_at` bookkeeping stamps (the same naming
--   discipline as the #375 issue projection).
-- - `base_sha` is the base branch head SHA where GitHub authoritatively
--   reported one (nullable; a fork or missing base ref may report none).
-- - `mergeable` is GitHub's documented nullable conflict-mergeability answer:
--   `true`/`false` once determined, NULL while GitHub has not yet determined
--   it (documented "not yet determined" — a truthful reading, never zero nor
--   false). `merge_state_status` is the documented `mergeable_state`
--   vocabulary verbatim (clean, dirty, draft, behind, blocked, unstable,
--   unknown); `unknown` is preserved verbatim as the provider's own
--   not-safely-observable reading. Conflict mergeability and the
--   policy/check-dependent merge state stay distinct facts — neither is ever
--   collapsed into a guaranteed can-merge boolean.
-- - `pr_mergeability_observed_at`/`pr_mergeability_head_sha` bind the last
--   mergeability observation to the exact head it was observed at: a stale
--   result is never presented as the new head's observation. Head changes
--   replace the binding only through a fresh successful reconciled
--   observation; uncertain/failed reconciliation writes nothing and the
--   last-observed pair persists truthfully until replaced.
--
-- Update paths: the existing update-only PR reconciliation (issue #63) writes
-- the richer snapshot through PR identity/head and PR-state webhook
-- deliveries, the check/status webhook path (for the affected record's
-- current head only), the scoped on-demand refresh (#375), and the existing
-- recovery sweeps. No scheduled polling and no per-check persistence: the
-- useful client-facing current PR merge state is this persisted summary.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.task_pull_requests
    add column title text,
    add column body text,
    add column html_url text,
    add column user_id bigint,
    add column user_login text,
    add column draft boolean,
    add column provider_created_at timestamptz,
    add column provider_updated_at timestamptz,
    add column provider_closed_at timestamptz,
    add column base_sha text,
    add column mergeable boolean,
    add column merge_state_status text,
    add column pr_mergeability_observed_at timestamptz,
    add column pr_mergeability_head_sha text;

-- The bounded user presentation subset carries the documented id/login pair
-- only, and both move together: a partially observed user is never written.
alter table openorc.task_pull_requests
    add constraint task_pull_requests_user_pair_coherence
    check ((user_id is null) = (user_login is null));

-- The mergeability observation evidence moves together: an observation
-- instant without its observed head (or the reverse) is not a readable
-- last-observed fact.
alter table openorc.task_pull_requests
    add constraint task_pull_requests_mergeability_observation_coherence
    check ((pr_mergeability_observed_at is null) = (pr_mergeability_head_sha is null));
