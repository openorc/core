-- Agent Runtime Connection default session capacity 2 (issue #200).
--
-- Phase 1 defaulted ``session_capacity`` to 1 so that concurrency had to be
-- explicitly enabled by the Owner, which assumed separate Connections for
-- the Producer and Reviewer roles. Completed Phase 2D qualification
-- established the v1 reference topology instead: one Cline Hub/Connection
-- serves both roles as distinct isolated Task sessions, and #68 admission
-- charges one slot per (Task, role) reservation. A correctly configured
-- shared-Hub Workspace therefore needs two capacity slots on its
-- Connection, and the old default would have prevented it from starting
-- any Task.
--
-- Forward-only, default-only migration: the column default changes from 1
-- to 2 and nothing else. No backfill runs and no existing row is rewritten:
-- stored capacities — including an explicitly configured 1, which remains a
-- valid dedicated single-role configuration — are preserved, along with
-- Owner edits and live reservations. The existing positive-capacity
-- validation from the initial migration stays in force, and the #68 atomic
-- all-or-nothing admission semantics are untouched. Capacity remains
-- Owner-configured admission control, never runtime-discovered state.
--
-- No privileges are given here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

alter table openorc.connections
    alter column session_capacity set default 2;
