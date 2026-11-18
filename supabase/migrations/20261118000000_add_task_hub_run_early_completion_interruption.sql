-- The staged early completion's interrupted-turn verdict (issue #342).
--
-- ``openorc.task_hub_run_early_completions`` stages one terminal observation
-- for its still-claimed receipt during the admission race (issue #322). The
-- detached Owner-Abort settlement (issue #342) consumes the observation's
-- neutral interrupted verdict — the concrete integration's own positive
-- classification of its qualified native interruption marker — when the
-- acknowledgement attach routes the staged observation, so the positive
-- interrupted-turn evidence survives the race exactly as delivered. The
-- column records that verdict only: no event name, payload, finish reason,
-- or any other native material is persisted, and the staged row is still
-- deleted at consumption so it never becomes permanent durable history.
--
-- No new privilege is granted here: the openorc schema lockout established
-- by the create_openorc_schema migration carries isolation, and runtime
-- access is backend-only.

alter table openorc.task_hub_run_early_completions
    add column interrupted boolean not null default false;
