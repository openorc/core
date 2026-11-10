# RQ Worker Agent Context

## Boundary

Workers own queue entrypoints, queue payload decoding, invocation of shared application services, and queue/transport integration.

They do not own business logic, workflow authority, the Task state machine, provider/runtime calls that bypass adapters/services, or durable copies of workflow truth.

## Shared operation invocation boundary (issue #86)

Logical operations cross process surfaces only through the strictly typed invocation boundary in `openorc.workers.invocation`: a closed operation-kind discriminator, kind-specific typed payloads, strict decoding before any effect, and one shared dispatcher (`invoke_operation`) with an explicit kind-to-handler table. The transport-facing submission seam distinguishes accepted-for-delivery, known refusal (typed reason), and uncertain submission; acceptance is never completion, external effect, or retry authorization, and queue/job IDs are never operation authority.

- Both execution modes use the same kind/handler mapping: regular mode through the bounded API-local executor owned by the serving process, queue mode through the canonical default queue and this worker process.
- The RQ worker process must be explicitly in queue execution mode and refuses regular-mode startup; mode selection alone never makes in-flight work safe to replay.
- The envelope and executor make no Task-state, currentness, or authorization decisions; that judgment belongs to the application services handlers invoke with process-local dependencies.
- The Task-workflow handlers delegate to the transport-neutral application coordinator (`openorc.services.task_workflow_operation_coordinator`), which invokes Level 1 through the single composition seam, preserves ordinary success, and on an applicable operational failure reports the invocation's own Level 1-produced attempt identity and currentness to the Level 2 triage service. Handlers never choose recovery, retry, or blocking, and Level 2's dispositions are authoritative over this surface; the coordinator re-raises the original exception so this process's sanitized, unchained worker-error report stays intact.
- The Task-less GitHub handlers (F9, issue #255) invoke the existing webhook-dispatch and reconciliation/recovery application services directly with process-local dependencies and only the decoded delivery GUID or scheduling tick. They are not enrolled in Level 2/3 operational-failure triage: their failures stay contained by this boundary's sanitized execution-error reporting, and the services' own durable outstanding-work semantics own recovery.

## Replay and durability rules

Assume every workflow-changing job can be delayed or replayed. The invoked service must revalidate current Task state, exact subject/gate/request, external side-effect completion, session binding, and authorization before acting.

A replayed job must never create replacement Task sessions, resend a known-delivered runtime message, create a second canonical PR, apply stale acceptance/gate decisions, or duplicate GitHub mutations.

RQ status is not workflow truth. Postgres is. Workers must be restartable without reconstructing domain state from process memory.

## Queue telemetry

- The worker process initializes observability once per process at start and performs the terminal shutdown/flush in `run_worker`'s `finally`; telemetry misconfiguration fails fast like other configuration errors, and runtime export failures never crash the worker.
- Job entrypoints create spans through `observability.tracing.application_tracer` with the RQ job identity from the safe attribute vocabulary (`openorc.rq_job_id`). Queue handoff may propagate trace context through job metadata where it can do so safely; queue state is never workflow truth and trace context is telemetry-only.

Read services plus the relevant adapter and persistence guides for queue behavior changes.

## Queue naming and backend contract

- `openorc:` is the stable application-level RQ queue prefix. Deployment isolation is provided by the Redis/Valkey namespace selected by `VALKEY_URL`. In the current reference/local setup this may be a Redis DB index; another deployment may use a dedicated instance/service.
- Canonical queue names are defined in `src/openorc/workers/queues.py`; the canonical default queue is `openorc:default`.
- The queue backend contract is Redis-compatibility through `VALKEY_URL`; no server binary or server version is pinned. The pinned `rq`/`redis` packages are application dependencies under the repository dependency policy, not server-version indicators.
