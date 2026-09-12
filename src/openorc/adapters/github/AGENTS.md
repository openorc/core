# GitHub Adapter Agent Context

## Boundary

This adapter owns GitHub API/webhook transport, normalization, stable GitHub identifiers, and authoritative-state reconciliation mechanics. Application services decide workflow consequences.

## Authority and reconciliation

- GitHub remains authoritative for issue requirements/relationships, committed code, PR/head state, CI/checks, branch protection, merge policy, and merge result.
- Webhook deliveries are notifications, not final truth. Validate signatures, deduplicate by delivery identity, then reconcile relevant state through GitHub APIs.
- Reconciliation must tolerate replay, missed delivery, and out-of-order delivery.
- Stable object IDs and exact commit SHAs matter more than mutable names/URLs.

## PR / review safety

- Canonical PR publication is an OpenOrc operation after explicit authority; the Producer supplies presentation content only.
- PR creation must use preflight branch-head validation and immediate post-create head reconciliation because GitHub creation lacks an atomic expected-head guard.
- Reviewer acceptance is exact-head addressed.
- Merge requests carry the exact reviewed/Owner-overridden head as expected SHA.
- CI/check projection presents GitHub-owned facts and must not become a second merge-policy engine.

Do not let GitHub comments, labels, or webhook payloads become implicit Producer authority.
