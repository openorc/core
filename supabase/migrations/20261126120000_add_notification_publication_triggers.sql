-- Application change notification publication triggers (G4b, issue #398).
--
-- Publishes transient, identifier-only change hints on the fixed internal
-- PostgreSQL notification channel established by G4a (issue #397,
-- ``openorc.notifications.contract.NOTIFICATION_CHANNEL``). The SQL side of
-- the payload contract lives ONLY in the two helpers below: every published
-- envelope is byte-identical to the canonical compact JSON shape produced by
-- ``openorc.notifications.contract.encode_notification_payload`` (fixed key
-- order with ``kind`` first, canonical lowercase hyphenated UUID strings, no
-- whitespace). Decoding accepts only the exact key sets, so shape drift is
-- caught by the notification integration suite and the G4c consumer.
--
-- Publication rules (issue #398 contract):
--
-- 1. Triggers only publish change hints. They perform no business writes, no
--    state inference, no audit generation, and no external I/O. Every trigger
--    function's entire effect is a pg_notify call through the two helpers.
-- 2. Native transaction semantics are reused as-is: pg_notify deliveries
--    become visible only after the outer commit; a transaction or savepoint
--    rollback suppresses its hints; identical channel+payload pairs coalesce
--    within one transaction (PostgreSQL-native). There is no notification
--    ledger, no replay, and no delivery guarantee.
-- 3. Conservative dependent-scope invalidation replaces reverse-dependency
--    enumeration: where one row's change can alter OTHER resources' public
--    projections, the trigger publishes a broader workspace/repository hint
--    derived from the changed row's own identity instead of enumerating
--    dependents. The closed resource vocabulary (workspace, project,
--    repository, issue, task, connection, role_binding, refresh) is never
--    extended.
-- 4. Genuine unchanged UPDATEs publish nothing (whole-row IS DISTINCT FROM
--    suppression inside the functions); the account-deletion barrier trigger
--    is additionally scoped by WHEN to exactly the account_deletion_state
--    column, so the lease-refresh write (attempt id and timestamp only) can
--    never publish.
-- 5. Functions live in the private openorc schema with fully qualified
--    references, are not SECURITY DEFINER, use no dynamic SQL, and are
--    revoked from PUBLIC. No privilege is granted to browser-facing roles:
--    the openorc schema lockout established by create_openorc_schema carries
--    isolation, and runtime access is backend-only. No new foreign keys and
--    no deletion-graph change; the existing blocked-task invariant trigger
--    machinery (20261104120000) is untouched.
--
-- Table-to-public-resource coverage map (INSERT/UPDATE/DELETE each publish
-- the changed row's NEW/OLD identity as applicable):
--
--   workspaces                     -> workspace (resource_id = workspace_id);
--                                     workspace DELETE publishes its identity
--                                     for stream invalidation
--   projects                       -> project
--   repositories                   -> repository
--   connections                    -> connection
--   workflow_role_bindings         -> role_binding (the binding row's UUID;
--                                     the Workspace+role API resolves it
--                                     through the unique (workspace_id, role))
--   github_installations           -> workspace (conservative: the row is
--                                     Workspace-scoped, so routed-repository
--                                     projections are covered by the
--                                     workspace hint; the repositories
--                                     routing-column change itself publishes
--                                     a repository hint)
--   workflow_events (INSERT only)  -> workflow_event hint (workspace_id,
--                                     workflow_event_id);
--                                     record_workflow_event remains the sole
--                                     application audit writer
--   tasks                          -> task, plus issue for the linked GitHub
--                                     issue projection (bounded unique
--                                     (repository_id, github_issue_id)
--                                     lookup) so intake/archive/purge and
--                                     eligibility changes cover affected
--                                     issue summaries and lists, plus
--                                     workspace when queue membership changes
--                                     (other queued Tasks' derived
--                                     queue_position and the Workspace queue
--                                     projection)
--
--   task_agent_sessions (including -> task (Task subrecords map to the task
--   the retained cost checkpoint      resource; internal-only receipt
--   writes), executions,              staging and raw native data are never
--   plan_revisions, review_loops,     published)
--   review_iterations, owner_gates,
--   task_blocks, task_pull_requests,
--   runtime_requests,
--   task_interrupt_intents,
--   task_resume_intents,
--   task_pull_request_close_intents,
--   task_runtime_send_claims,
--   task_hub_run_receipts
--   github_issues                  -> issue (internal issue UUID) plus a
--                                     conservative workspace hint: cross-
--                                     repository related-issue presentation
--                                     and current AND archived Task attempts
--                                     embedding the linked issue's projection
--                                     change without their own rows changing
--   github_issue_hierarchy,        -> the anchored endpoint's issue hint
--   github_issue_dependencies,        (bounded unique lookup) plus a
--   github_issue_sub_issues           conservative workspace hint (cross-
--                                     repository presentation and unobserved
--                                     related endpoints)
--   github_refreshes               -> refresh, plus repository when
--                                     repository_id is not null and workspace
--                                     otherwise; issue/task rewrites performed
--                                     by a refresh publish through their own
--                                     triggers
--   github_user_authorizations     -> workspace hints fanned out only to the
--                                     Workspaces owned by the Profile
--                                     (authoritative ownership lookup)
--   profiles (UPDATE, WHEN scoped  -> workspace hints via the same ownership
--   to account_deletion_state)        fan-out: establishing or clearing the
--                                     account-deletion barrier changes Task
--                                     and OwnerGate action eligibility across
--                                     the Profile's Workspaces without their
--                                     rows changing
--
-- Explicitly omitted (internal-only; no public projection change):
-- task_hub_run_early_completions (internal-only early-completion staging
-- consumed by the attach path; the accompanying receipt/observation writes
-- publish), github_user_authorization_flows (completion publishes through the
-- authorization row), github_webhook_deliveries and
-- github_webhook_delivery_routes (dedupe/routing ledger; reconciliation
-- effects publish through the projection triggers), operational_failure_episodes
-- and operational_failure_attempts (escalations publish through the block/gate
-- triggers). No notification is published for internal secret rotation alone.

create function openorc.notify_data_changed (
    p_workspace_id uuid,
    p_resource_type text,
    p_resource_id uuid
) returns void
language plpgsql
as $notifier$
begin
    perform pg_notify(
        'openorc_notifications',
        '{"kind":"data_changed","workspace_id":"' || p_workspace_id::text
            || '","resource_type":"' || p_resource_type
            || '","resource_id":"' || p_resource_id::text || '"}'
    );
end;
$notifier$;

create function openorc.notify_workflow_event (
    p_workspace_id uuid,
    p_workflow_event_id uuid
) returns void
language plpgsql
as $notifier$
begin
    perform pg_notify(
        'openorc_notifications',
        '{"kind":"workflow_event","workspace_id":"' || p_workspace_id::text
            || '","workflow_event_id":"' || p_workflow_event_id::text || '"}'
    );
end;
$notifier$;

-- Generic single-row dispatcher for the simple one-row-per-resource families.
-- Trigger arguments: (resource_type, identity column name). The UPDATE
-- suppression is whole-row, so a genuine unchanged write publishes nothing.
create function openorc.notify_row_data_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_row jsonb;
    v_workspace_id uuid;
    v_resource_id uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
        v_row := to_jsonb(new);
    elsif tg_op = 'DELETE' then
        v_row := to_jsonb(old);
    else
        v_row := to_jsonb(new);
    end if;
    v_workspace_id := (v_row ->> 'workspace_id')::uuid;
    v_resource_id := (v_row ->> tg_argv[1])::uuid;
    if v_workspace_id is null or v_resource_id is null then
        raise exception
            'notification trigger % on openorc.% resolved no workspace or % identity',
            tg_name,
            tg_table_name,
            tg_argv[1];
    end if;
    perform openorc.notify_data_changed(v_workspace_id, tg_argv[0], v_resource_id);
    return null;
end;
$notifier$;

-- Workspace aggregate root: the workspaces table has no workspace_id column
-- (its primary key id IS the Workspace identity), so this dedicated trigger
-- uses the row's id for both envelope UUIDs. The generic dispatcher is never
-- attached to this table.
create function openorc.notify_workspace_root_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_workspace_id uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
    end if;
    if tg_op = 'DELETE' then
        v_workspace_id := old.id;
    else
        v_workspace_id := new.id;
    end if;
    perform openorc.notify_data_changed(v_workspace_id, 'workspace', v_workspace_id);
    return null;
end;
$notifier$;

-- Task aggregate root: task hint always, issue hint for the linked GitHub
-- issue projection (Task intake/archive/purge and eligibility changes must
-- cover affected issue summaries and lists), and a workspace hint when queue
-- membership changes (other queued Tasks' derived queue_position and the
-- Workspace queue projection change without their rows changing).
create function openorc.notify_task_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_workspace_id uuid;
    v_task_id uuid;
    v_repository_id uuid;
    v_github_issue_id bigint;
    v_entry_queued_at timestamptz;
    v_exit_queued_at timestamptz;
    v_issue_uuid uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
    end if;
    if tg_op = 'DELETE' then
        v_workspace_id := old.workspace_id;
        v_task_id := old.id;
        v_repository_id := old.repository_id;
        v_github_issue_id := old.github_issue_id;
        v_entry_queued_at := old.queued_at;
        v_exit_queued_at := null;
    else
        v_workspace_id := new.workspace_id;
        v_task_id := new.id;
        v_repository_id := new.repository_id;
        v_github_issue_id := new.github_issue_id;
        v_exit_queued_at := new.queued_at;
        if tg_op = 'INSERT' then
            v_entry_queued_at := null;
        else
            v_entry_queued_at := old.queued_at;
        end if;
    end if;
    perform openorc.notify_data_changed(v_workspace_id, 'task', v_task_id);
    if v_entry_queued_at is distinct from v_exit_queued_at then
        perform openorc.notify_data_changed(v_workspace_id, 'workspace', v_workspace_id);
    end if;
    select gi.id into v_issue_uuid
        from openorc.github_issues gi
        where gi.repository_id = v_repository_id
          and gi.github_issue_id = v_github_issue_id;
    if v_issue_uuid is not null then
        perform openorc.notify_data_changed(v_workspace_id, 'issue', v_issue_uuid);
    end if;
    return null;
end;
$notifier$;

-- GitHub issue projection: issue hint plus a conservative workspace hint.
create function openorc.notify_github_issue_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_workspace_id uuid;
    v_issue_id uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
    end if;
    if tg_op = 'DELETE' then
        v_workspace_id := old.workspace_id;
        v_issue_id := old.id;
    else
        v_workspace_id := new.workspace_id;
        v_issue_id := new.id;
    end if;
    perform openorc.notify_data_changed(v_workspace_id, 'issue', v_issue_id);
    perform openorc.notify_data_changed(v_workspace_id, 'workspace', v_workspace_id);
    return null;
end;
$notifier$;

-- Relationship mirror edges (hierarchy/dependency/sub-issue): the anchored
-- endpoint's issue hint via the bounded unique lookup, plus a conservative
-- workspace hint covering cross-repository presentation and unobserved
-- related endpoints. All three tables share the same anchored column shape.
create function openorc.notify_github_issue_relation_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_workspace_id uuid;
    v_repository_id uuid;
    v_github_issue_id bigint;
    v_issue_uuid uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
    end if;
    if tg_op = 'DELETE' then
        v_workspace_id := old.workspace_id;
        v_repository_id := old.repository_id;
        v_github_issue_id := old.github_issue_id;
    else
        v_workspace_id := new.workspace_id;
        v_repository_id := new.repository_id;
        v_github_issue_id := new.github_issue_id;
    end if;
    select gi.id into v_issue_uuid
        from openorc.github_issues gi
        where gi.repository_id = v_repository_id
          and gi.github_issue_id = v_github_issue_id;
    if v_issue_uuid is not null then
        perform openorc.notify_data_changed(v_workspace_id, 'issue', v_issue_uuid);
    end if;
    perform openorc.notify_data_changed(v_workspace_id, 'workspace', v_workspace_id);
    return null;
end;
$notifier$;

-- GitHub refresh lifecycle: refresh hint plus the addressed scope.
create function openorc.notify_github_refresh_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_workspace_id uuid;
    v_repository_id uuid;
    v_refresh_id uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
    end if;
    if tg_op = 'DELETE' then
        v_workspace_id := old.workspace_id;
        v_repository_id := old.repository_id;
        v_refresh_id := old.id;
    else
        v_workspace_id := new.workspace_id;
        v_repository_id := new.repository_id;
        v_refresh_id := new.id;
    end if;
    perform openorc.notify_data_changed(v_workspace_id, 'refresh', v_refresh_id);
    if v_repository_id is not null then
        perform openorc.notify_data_changed(v_workspace_id, 'repository', v_repository_id);
    else
        perform openorc.notify_data_changed(v_workspace_id, 'workspace', v_workspace_id);
    end if;
    return null;
end;
$notifier$;

-- Profile-scoped GitHub user authorization: conservative fan-out to the
-- Workspaces the authorizing Profile owns. No credential, Vault reference, or
-- OAuth state is ever published.
create function openorc.notify_github_user_authorization_changed () returns trigger
language plpgsql
as $notifier$
declare
    v_profile_id uuid;
begin
    if tg_op = 'UPDATE' then
        if old is not distinct from new then
            return null;
        end if;
    end if;
    if tg_op = 'DELETE' then
        v_profile_id := old.profile_id;
    else
        v_profile_id := new.profile_id;
    end if;
    perform openorc.notify_data_changed(w.id, 'workspace', w.id)
        from openorc.workspaces w
        where w.owner_profile_id = v_profile_id;
    return null;
end;
$notifier$;

-- Account-deletion barrier: the account_operationally_offered predicate
-- gates Task and OwnerGate actions across the Profile's Workspaces, so
-- establishing or clearing the barrier fans out workspace hints. The trigger
-- is WHEN-scoped to account_deletion_state (lease-refresh writes publish
-- nothing); account-root deletion fans out through the workspaces cascade.
create function openorc.notify_profile_account_barrier_changed () returns trigger
language plpgsql
as $notifier$
begin
    perform openorc.notify_data_changed(w.id, 'workspace', w.id)
        from openorc.workspaces w
        where w.owner_profile_id = new.id;
    return null;
end;
$notifier$;

-- WorkflowEvent audit insertion: one workflow_event hint per committed event.
create function openorc.notify_workflow_event_inserted () returns trigger
language plpgsql
as $notifier$
begin
    perform openorc.notify_workflow_event(new.workspace_id, new.id);
    return null;
end;
$notifier$;

-- Simple one-row-per-resource configuration and projection families. The
-- Workspace root uses its dedicated identity function (see above).
create trigger notify_workspaces_changed
    after insert or update or delete on openorc.workspaces
    for each row execute function openorc.notify_workspace_root_changed ();

create trigger notify_projects_changed
    after insert or update or delete on openorc.projects
    for each row execute function openorc.notify_row_data_changed ('project', 'id');

create trigger notify_repositories_changed
    after insert or update or delete on openorc.repositories
    for each row execute function openorc.notify_row_data_changed ('repository', 'id');

create trigger notify_connections_changed
    after insert or update or delete on openorc.connections
    for each row execute function openorc.notify_row_data_changed ('connection', 'id');

create trigger notify_workflow_role_bindings_changed
    after insert or update or delete on openorc.workflow_role_bindings
    for each row execute function openorc.notify_row_data_changed ('role_binding', 'id');

-- Installation observation/claim changes conservatively invalidate the
-- Workspace (see the coverage map above).
create trigger notify_github_installations_changed
    after insert or update or delete on openorc.github_installations
    for each row execute function openorc.notify_row_data_changed ('workspace', 'workspace_id');

-- Task aggregate subrecords map to the task resource.
create trigger notify_task_agent_sessions_changed
    after insert or update or delete on openorc.task_agent_sessions
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_executions_changed
    after insert or update or delete on openorc.executions
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_plan_revisions_changed
    after insert or update or delete on openorc.plan_revisions
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_review_loops_changed
    after insert or update or delete on openorc.review_loops
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_review_iterations_changed
    after insert or update or delete on openorc.review_iterations
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_owner_gates_changed
    after insert or update or delete on openorc.owner_gates
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_blocks_changed
    after insert or update or delete on openorc.task_blocks
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_pull_requests_changed
    after insert or update or delete on openorc.task_pull_requests
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_runtime_requests_changed
    after insert or update or delete on openorc.runtime_requests
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_interrupt_intents_changed
    after insert or update or delete on openorc.task_interrupt_intents
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_resume_intents_changed
    after insert or update or delete on openorc.task_resume_intents
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_pull_request_close_intents_changed
    after insert or update or delete on openorc.task_pull_request_close_intents
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_runtime_send_claims_changed
    after insert or update or delete on openorc.task_runtime_send_claims
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

create trigger notify_task_hub_run_receipts_changed
    after insert or update or delete on openorc.task_hub_run_receipts
    for each row execute function openorc.notify_row_data_changed ('task', 'task_id');

-- Task aggregate root and derived scopes.
create trigger notify_tasks_changed
    after insert or update or delete on openorc.tasks
    for each row execute function openorc.notify_task_changed ();

-- GitHub issue projection and relationship mirrors.
create trigger notify_github_issues_changed
    after insert or update or delete on openorc.github_issues
    for each row execute function openorc.notify_github_issue_changed ();

create trigger notify_github_issue_hierarchy_changed
    after insert or update or delete on openorc.github_issue_hierarchy
    for each row execute function openorc.notify_github_issue_relation_changed ();

create trigger notify_github_issue_dependencies_changed
    after insert or update or delete on openorc.github_issue_dependencies
    for each row execute function openorc.notify_github_issue_relation_changed ();

create trigger notify_github_issue_sub_issues_changed
    after insert or update or delete on openorc.github_issue_sub_issues
    for each row execute function openorc.notify_github_issue_relation_changed ();

-- GitHub refresh lifecycle.
create trigger notify_github_refreshes_changed
    after insert or update or delete on openorc.github_refreshes
    for each row execute function openorc.notify_github_refresh_changed ();

-- Profile-scoped GitHub user authorization fan-out.
create trigger notify_github_user_authorizations_changed
    after insert or update or delete on openorc.github_user_authorizations
    for each row execute function openorc.notify_github_user_authorization_changed ();

-- Account-deletion barrier, scoped by WHEN to exactly the state column.
create trigger notify_profiles_account_barrier_changed
    after update on openorc.profiles
    for each row
    when (old.account_deletion_state is distinct from new.account_deletion_state)
    execute function openorc.notify_profile_account_barrier_changed ();

-- WorkflowEvent audit insertion (INSERT only; the audit writer is unchanged).
create trigger notify_workflow_events_inserted
    after insert on openorc.workflow_events
    for each row execute function openorc.notify_workflow_event_inserted ();

revoke execute on function openorc.notify_data_changed (uuid, text, uuid) from public;
revoke execute on function openorc.notify_workflow_event (uuid, uuid) from public;
revoke execute on function openorc.notify_row_data_changed () from public;
revoke execute on function openorc.notify_workspace_root_changed () from public;
revoke execute on function openorc.notify_task_changed () from public;
revoke execute on function openorc.notify_github_issue_changed () from public;
revoke execute on function openorc.notify_github_issue_relation_changed () from public;
revoke execute on function openorc.notify_github_refresh_changed () from public;
revoke execute on function openorc.notify_github_user_authorization_changed () from public;
revoke execute on function openorc.notify_profile_account_barrier_changed () from public;
revoke execute on function openorc.notify_workflow_event_inserted () from public;
