-- OpenOrc WorkflowRoleBinding runtime session configuration (issue #162).
--
-- Adds the concrete per-role runtime-session configuration that Phase 1
-- deliberately deferred until a demonstrated configurable property existed
-- (the managed-Cline R3/R5 contract established the durable inputs). Three
-- additive columns on openorc.workflow_role_bindings — no replacement table,
-- no provider/model enum or foreign catalog table, no generic runtime
-- configuration JSON bag, and no prompt hash/version/history:
--
-- - configured_provider / configured_model — the Owner-supplied opaque
--   runtime identity pair for the role's sessions. Pure configuration
--   authority: OpenOrc passes the values to the adapter as-is and never
--   validates them through any provider/model catalog, configured-provider
--   enumeration, private runtime state, or effective-identity readback. They
--   are deliberately distinct from the Connection's nullable runtime-reported
--   reported_provider/reported_model observations. The pair is complete only
--   when both values are present; a partial one-value configuration cannot
--   be committed. Existing/legacy bindings migrate as entirely unconfigured
--   (both NULL) with no invented default — runtime/session construction may
--   fail closed later on an unconfigured pair, but persistence never
--   fabricates a provider or model.
-- - role_prompt_override — the nullable Owner-authored role-prompt Markdown
--   override. NULL means OpenOrc's current shipped default for that role
--   applies; a non-NULL value — including an empty or whitespace-only string
--   — is the Owner's explicit override, stored and passed verbatim without
--   parsing, classification, or blankness rules. The shipped default prompt
--   bodies remain OpenOrc-owned code assets and are deliberately never
--   materialized into ordinary persistence; there is no prompt hash,
--   version, revision, snapshot, or history of any kind.
--
-- Live Workspace configuration semantics: editing these values is an
-- ordinary Workspace configuration mutation. It never mutates an
-- already-resident runtime incarnation and never rewrites an existing
-- TaskAgentSession, its admitted Connection boundary, or its historical
-- effective configuration snapshot; later legitimate runtime construction
-- or same-ID reconstruction resolves the then-current configuration.
--
-- No privileges are given here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.workflow_role_bindings
    add column configured_provider text,
    add column configured_model text,
    add column role_prompt_override text;

alter table openorc.workflow_role_bindings
    add constraint workflow_role_bindings_provider_model_pair_check
        check ((configured_provider is null) = (configured_model is null)),
    add constraint workflow_role_bindings_provider_nonblank_check
        check (configured_provider is null or configured_provider ~ '\S'),
    add constraint workflow_role_bindings_model_nonblank_check
        check (configured_model is null or configured_model ~ '\S');
