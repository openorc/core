-- Owner spending authorization: the Workspace default Task budget (issue #240,
-- Pre-F cost accounting).
--
-- Adds the third first-class, typed Workspace configuration setting alongside
-- the issue #53 review_iteration_limit and guidance columns: a nullable,
-- positive USD default Task budget the Owner authorizes for each Task the
-- Workspace starts.
--
-- Durable properties carried by this migration:
--
-- - ``default_task_budget_usd numeric`` is deliberately nullable: NULL means
--   spending policy is disabled for this Workspace and there is no implicit
--   limit — absence is the ordinary unconfigured case, never zero. The CHECK
--   admits only NULL or a strictly positive amount; zero and negative values
--   are configuration errors, not "unlimited".
-- - The value is exact Postgres ``numeric`` and is carried end to end as
--   Python ``decimal.Decimal`` (never a binary float), so an Owner-configured
--   amount is never silently re-rounded by storage or transport. This is
--   best-effort observed-spend budgeting, not a billing-grade financial
--   ledger: no price tables, no per-turn accounting, no cost forecasts, and
--   no token-to-dollar conversion exist anywhere in this schema.
-- - The default is copied onto each Task as its authorized budget exactly
--   once, by the successful Owner Start that transitions the Task out of
--   ``ready_to_plan`` (the companion task-authorized-budget migration).
--   Editing this default later affects only future Tasks: an already-started
--   Task keeps the authorization it was started with, and the separate F4b
--   budget-increase command (issue #230) owns any later change to a started
--   Task's authorization.
-- - No backfill: existing Workspaces keep NULL (disabled). No API/UI surface
--   is introduced by this migration; the Owner-facing operation arrives
--   through the existing Workspace configuration capability.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.workspaces
    add column default_task_budget_usd numeric
    check (default_task_budget_usd is null or default_task_budget_usd > 0);