-- E9 durable one-shot remediation claim association (issue #84).
--
-- One additive nullable column on the historical attempt record plus two
-- focused constraints:
--
-- - executions.review_iteration_id: the finalized CHANGES_REQUESTED
--   ReviewIteration whose Producer remediation this Execution attempt is.
--   NULL for every attempt that is not a PR-remediation continuation (E6's
--   initial authorized implementation attempt keeps NULL; the column is
--   written only by the E9 remediation claim). The composite foreign key
--   keeps the claimed iteration in the same Task/Workspace scope as the
--   attempt, mirroring the existing composite-subject foreign keys.
-- - unique (id, task_id, workspace_id) on openorc.review_iterations: the
--   identity hook the composite foreign key references (mirrors the
--   plan-revision and review-loop hooks).
-- - the partial unique index on executions.review_iteration_id: at most ONE
--   attempt can ever claim a given finalized ReviewIteration for remediation
--   — completed, known-failed, and uncertain attempts alike. This is the
--   durable one-shot invariant behind the E9 Producer remediation claim,
--   independent of any Task state token: a replayed or raced second claim
--   for the same ReviewIteration can never commit a second attempt row.
--
-- No other column, table, constraint, index, or privilege changes; the
-- openorc schema remains locked to non-OpenOrc roles (no new grants).
alter table openorc.review_iterations
    add constraint review_iterations_identity_hook
    unique (id, task_id, workspace_id);

alter table openorc.executions
    add column review_iteration_id uuid;

alter table openorc.executions
    add constraint executions_review_iteration_fk
    foreign key (review_iteration_id, task_id, workspace_id)
    references openorc.review_iterations (id, task_id, workspace_id);

create unique index executions_review_iteration_claim_idx
    on openorc.executions (review_iteration_id)
    where review_iteration_id is not null;
