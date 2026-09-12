# RQ Worker Agent Context

## Boundary

Workers own queue entrypoints, queue payload decoding, invocation of shared application services, and queue/transport integration.

They do not own business logic, workflow authority, the Task state machine, provider/runtime calls that bypass adapters/services, or durable copies of workflow truth.

## Replay and durability rules

Assume every workflow-changing job can be delayed or replayed. The invoked service must revalidate current Task state, exact subject/gate/request, external side-effect completion, session binding, and authorization before acting.

A replayed job must never create replacement Task sessions, resend a known-delivered runtime message, create a second canonical PR, apply stale acceptance/gate decisions, or duplicate GitHub mutations.

RQ status is not workflow truth. Postgres is. Workers must be restartable without reconstructing domain state from process memory.

Read services plus the relevant adapter and persistence guides for queue behavior changes.
