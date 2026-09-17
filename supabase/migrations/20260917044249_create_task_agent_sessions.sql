-- OpenOrc Task agent session persistence (Phase 1, issue #22).
--
-- Establishes the durable Task/role <-> external-session binding that
-- preserves Task-scoped conversational continuity without introducing any
-- runtime behavior:
--
--   Workspace -> Repository -> Task -> TaskAgentSession -> Connection
--
-- Durable properties carried by this migration:
--
-- - One TaskAgentSession binding per (task_id, role) in v1 for PRODUCER and
--   REVIEWER. The unique constraint makes repeated establishment unable to
--   create a second row: establishment is idempotent for the same Connection
--   and a deterministic conflict against a different one. There is no
--   binding replacement in v1 — a Task never swaps its Producer or Reviewer
--   session binding.
-- - ``external_session_id`` is the opaque external-session identity bound by
--   the runtime. It is NULL while the binding is CONNECTING (no confirmed
--   external session exists yet) and, once successfully initialized, is
--   immutable for the binding's lifetime: no code path overwrites it with a
--   replacement session. The partial unique index makes one non-null
--   external session identity belong to exactly one Task/role binding within
--   its Connection — the same opaque identity cannot be reused across Tasks
--   or roles sharing that Connection. Identity is scoped per Connection: a
--   different Connection may independently bind the same opaque string.
-- - Lifecycle vocabulary is CONNECTING, READY, LOST, ENDED (text + CHECK,
--   never a native enum). CONNECTING is establishment in progress and is by
--   definition not yet a bound session. READY means the external session was
--   successfully initialized. LOST records genuine loss of the exact
--   external session/context on the SAME binding — it is lifecycle history,
--   never a trigger for persistence-level replacement; later workflow
--   services translate it into AGENT_SESSION_LOST blocking behavior. ENDED
--   is normal session termination, reachable before initialization (the
--   establishment attempt ends without ever binding a session) or after
--   initialization. Runtime/Hub unavailability is deliberately NOT lifecycle
--   state here: a recoverable Hub restart does not create a replacement
--   binding, and reachability/health is separate telemetry, never durable
--   workflow state.
-- - Initialization coherence is CHECK-enforced in both directions:
--   CONNECTING requires NULL ``external_session_id`` AND NULL
--   ``initialized_at``; READY and LOST require both non-NULL; ENDED permits
--   either coherent form. Mixed forms (one NULL, one non-NULL) match no
--   branch and are rejected for every lifecycle status.
-- - ``ended_at`` is the semantic ENDED timestamp: non-NULL exactly when the
--   lifecycle status is ENDED. It is never substituted by ``updated_at``.
-- - ``initialization_protocol_version`` records the protocol version used to
--   initialize the external session (opaque version string; absence is
--   valid).
-- - ``effective_config_snapshot`` is the NON-SECRET effective runtime/session
--   configuration snapshot captured at initialization. It is assembled by
--   the caller that establishes the session; it must never be populated by
--   blindly serializing Connection configuration or authentication material,
--   and raw credentials/tokens must never enter this column. It is validated
--   for canonical JSON-object form only. Once the binding has initialized,
--   the snapshot is historical for that Task session: later Workspace
--   role-binding/Connection configuration changes affect future sessions and
--   never rewrite an initialized session's snapshot (no update path touches
--   it after initialization).
-- - ``reported_provider``/``reported_model``/``reported_runtime_version``
--   are nullable opaque runtime-reported provenance observations — arbitrary
--   strings or NULL, never enums, never configuration authority. Absence is
--   valid.
-- - Capacity accounting is Connection-scoped: Producer and Reviewer sessions
--   sharing one Connection each consume occupancy against that Connection's
--   Owner-configured ``session_capacity``. The partial active-session index
--   below serves that accounting (active means CONNECTING or READY);
--   admission decisions belong to later workflow services.
-- - ``workspace_id`` is carried directly on the Workspace-owned row, and the
--   composite foreign keys keep the direct scope in agreement with both the
--   Task's and the Connection's Workspace. ``openorc.tasks`` gains the
--   unique (id, workspace_id) hook here because only projects, connections,
--   and repositories carried it until now.
-- - Ordinary lifecycle archives Tasks; this table is Task-scoped internal
--   history. Explicit Owner purge of archived internal Task data is a
--   distinct, separately authorized operation that never implies GitHub
--   mutation.
-- - No privileges are given to browser-facing roles: the schema-level lockout
--   established by the create_openorc_schema migration carries isolation, and
--   runtime access is backend-only.

-- Hook for Workspace-owned children of a Task: lets the session binding's
-- direct Workspace scope be constrained to agree with Task ownership, exactly
-- like the projects, connections, and repositories hooks.
alter table openorc.tasks
    add constraint tasks_id_workspace_id_uniq unique (id, workspace_id);

create table openorc.task_agent_sessions (
    id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references openorc.workspaces (id),
    task_id uuid not null,
    role text not null check (role in ('producer', 'reviewer')),
    connection_id uuid not null,
    -- Opaque external-session identity: NULL until a successful
    -- initialization, NULL-or-nonblank, immutable once set.
    external_session_id text check (external_session_id is null or external_session_id ~ '\S'),
    lifecycle_status text not null check (lifecycle_status in (
        'connecting', 'ready', 'lost', 'ended'
    )),
    -- Opaque initialization protocol version; NULL until initialization and
    -- NULL-or-nonblank. Absence is valid.
    initialization_protocol_version text check (
        initialization_protocol_version is null or initialization_protocol_version ~ '\S'
    ),
    -- Non-secret effective runtime/session configuration snapshot captured at
    -- initialization (canonical JSON object). Never populated by blindly
    -- serializing Connection configuration or authentication material; raw
    -- credentials/tokens must never enter this column. Historical for the
    -- initialized binding: later Connection/role-binding configuration
    -- changes never rewrite it.
    effective_config_snapshot jsonb check (jsonb_typeof(effective_config_snapshot) = 'object'),
    -- Nullable opaque runtime-reported provenance observations.
    reported_provider text,
    reported_model text,
    reported_runtime_version text,
    -- Semantic lifecycle timestamps. ``initialized_at`` is stamped once by
    -- the CONNECTING -> READY initialization. ``ended_at`` is non-NULL
    -- exactly when the lifecycle status is ENDED.
    initialized_at timestamptz,
    ended_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- Direct Workspace scope must agree with the Task's Workspace.
    foreign key (task_id, workspace_id)
        references openorc.tasks (id, workspace_id),

    -- Direct Workspace scope must agree with the bound Connection's Workspace.
    foreign key (connection_id, workspace_id)
        references openorc.connections (id, workspace_id),

    -- One binding per Task role in v1: repeated establishment cannot create
    -- a second row (idempotent reuse for the same Connection, deterministic
    -- conflict otherwise). Also serves Task-role lookup.
    unique (task_id, role),

    -- Initialization coherence (see header): CONNECTING requires both NULL;
    -- READY/LOST require both non-NULL; ENDED permits either coherent form;
    -- mixed forms are rejected for every lifecycle status.
    check (
        (
            lifecycle_status = 'connecting'
            and external_session_id is null
            and initialized_at is null
        )
        or (
            lifecycle_status in ('ready', 'lost')
            and external_session_id is not null
            and initialized_at is not null
        )
        or (
            lifecycle_status = 'ended'
            and (
                (external_session_id is null and initialized_at is null)
                or (external_session_id is not null and initialized_at is not null)
            )
        )
    ),

    -- ``ended_at`` is non-NULL exactly when the lifecycle status is ENDED.
    check (
        (lifecycle_status = 'ended' and ended_at is not null)
        or (lifecycle_status <> 'ended' and ended_at is null)
    )
);

-- One non-null external session identity belongs to exactly one Task/role
-- binding within its Connection: the same opaque identity cannot be reused
-- across Tasks or roles sharing that Connection. Uninitialized bindings
-- (NULL identity) are unrestricted.
create unique index task_agent_sessions_connection_external_session_uniq
    on openorc.task_agent_sessions (connection_id, external_session_id)
    where external_session_id is not null;

-- Connection-scoped capacity accounting: active occupancy (CONNECTING or
-- READY) per Connection, compared against the Connection's Owner-configured
-- session_capacity by later admission services.
create index task_agent_sessions_connection_active_idx
    on openorc.task_agent_sessions (connection_id)
    where lifecycle_status in ('connecting', 'ready');

-- Active session queries across a Workspace.
create index task_agent_sessions_workspace_active_idx
    on openorc.task_agent_sessions (workspace_id)
    where lifecycle_status in ('connecting', 'ready');
