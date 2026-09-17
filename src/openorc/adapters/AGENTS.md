# Adapter Layer Agent Context

## Boundary

Adapters translate between OpenOrc semantic operations and external systems. They own external transport/session mechanics, provider-native request/response handling, normalization, and protocol/transport error classification.

Adapters do not own Task-state semantics, Owner authority, review policy, workflow transitions, or product retry decisions.

## Rules

- Return normalized typed results/errors to application services.
- Harmless presentation normalization is allowed; semantic invention is not.
- Never manufacture structured OpenOrc meaning from unstructured model prose.
- Agent Runtime adapters (e.g. Cline) own runtime-native response extraction, normalization, and provider/runtime error classification locally, and consume shared `src/openorc/protocol/` validation for formal OpenOrc contracts rather than duplicating formal OpenOrc schemas here; non-runtime external-system adapters (e.g. GitHub) have no agent-protocol dependency.
- Preserve exact external identifiers needed for deterministic routing/reconciliation.
- Treat uncertain delivery/outcome explicitly; do not pretend timeout means non-delivery.
- Keep runtime/provider-specific capabilities behind local adapter boundaries rather than expanding the universal contract casually.

Read the provider-specific nested guide plus services/domain guidance for cross-boundary changes.
