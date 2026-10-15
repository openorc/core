-- OpenOrc Connection-level reported-observation retirement (issue #188).
--
-- Removes the two retired placeholder observation columns from
-- openorc.connections — the Phase 1 Connection-level ``reported_provider``/
-- ``reported_model`` pair. They were a retired placeholder for a Cline
-- catalog/readback API that never became a contract: configured provider/model
-- identity is the Owner-supplied opaque WorkflowRoleBinding pair (issue #162),
-- and optional runtime-reported provenance lives on TaskAgentSession
-- (reported_provider/reported_model/reported_runtime_version), which this
-- migration deliberately does not touch — session history and its provenance
-- observations are never rewritten.
--
-- Forward-only migration with no backfill: no endpoint value is invented for
-- existing rows, and no other column, table, constraint, or privilege is
-- touched. Connections whose ``safe_config`` carries no ``control_endpoint``
-- remain stored exactly as they are; they are simply not attachable until
-- the Owner configures the endpoint (issue #188). No data migration runs,
-- and row identity, Workspace scope, role bindings, session capacity,
-- credential references, and ``repository_base`` are preserved untouched.
--
-- The endpoint configuration itself lives inside the existing ``safe_config``
-- JSON container under the adapter-defined v1 schema; there is deliberately
-- no new column, no per-adapter table hierarchy, and no SQL-level URL
-- validation — parsing/validation of the Cline control endpoint is the
-- adapter-boundary primitive (issue #188), never database shape.
--
-- No privileges are given here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.connections
    drop column reported_provider,
    drop column reported_model;

