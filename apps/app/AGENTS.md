# Product SPA Agent Context

## Boundary

`apps/app` is the authenticated OpenOrc product SPA. The name `web` is reserved for a future public/marketing site.

Own Vue 3/Vite/TypeScript UI, Owner-facing views, client-only UI state, server-state query/mutation integration, authorized SSE consumption, and links to authoritative external systems.

Do not implement independent workflow authority, direct Agent Runtime calls, direct GitHub workflow mutations, or direct Supabase application-data access that bypasses the OpenOrc API.

## Framework baseline

- Vue 3 Composition API with `<script setup lang="ts">`.
- TypeScript strictness.
- PrimeVue for the component system.
- TanStack Vue Query owns server-derived state.
- Pinia is for genuine client/application state, not a duplicate server truth store.

## Product invariants

- The dashboard is the primary happy path.
- Each active Task wizard occupies its own tab; never introduce a singleton global current Task.
- Owner Requests surface durable gates and scoped runtime approvals.
- Reviewer discussion is Owner ↔ Reviewer only; never create free-form Owner ↔ Producer chat.
- Implementation authorization and merge are explicit actions tied to the exact current subject.
- Durable blocked/recovery conditions remain visible; they are not toast-only errors.
- Runtime telemetry may legitimately be `UNKNOWN`; runtime capacity is distinct from Task-session health.
- External GitHub/runtime/provider links are navigation affordances, not hidden application-data transports.
- Mobile-friendly/PWA posture must not create offline workflow authority.

## Errors and verification

Use a centralized transient API-error/toast path. Add frontend component/state/API-contract tests appropriate to changed behavior.

API contract changes require reading `src/openorc/api/AGENTS.md` and relevant service/domain guidance.
