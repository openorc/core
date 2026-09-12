# GitHub / CI Agent Context

## Boundary

This subtree owns repository CI, GitHub automation, issue/PR templates if adopted, dependency/release automation, workflow permissions, and repository workflow names coupled to protection rules.

It does not own OpenOrc Cloud production deployment.

## Invariants

- After bootstrap governance is enabled, normal implementation changes use branches and pull requests to protected `main`.
- Keep workflow/job names stable once branch protection depends on them.
- Use least-privilege GitHub Actions permissions.
- Never embed real secrets in workflow files.
- Prefer clean repository-wide quality gates over legacy changed-files-only exceptions.
- Do not invent label or release taxonomies merely because another repository has them.

## Merge/update policy

The actual GitHub repository settings are authoritative. Until governance is configured and documented here, do not assume a merge method or branch-update strategy.

Agents do not merge implementation PRs unless repository policy explicitly changes.

## Cross-boundary checks

CI/test tooling changes must also read `tests/AGENTS.md` and the guide for the subsystem being exercised.
