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

## Dependabot

- `.github/dependabot.yml` is the maintained version-update configuration. It does not enable auto-merge; Dependabot PRs merge through normal review and branch protection.
- Repository settings (Settings → Code security and analysis): Dependabot alerts and Dependabot security updates are enabled. Security remediation PRs open immediately, are never grouped, and are never auto-merged. The `cooldown` option does not apply to security updates.
- Cadence: `uv` (`/`), npm (`/apps/app`), and GitHub Actions (`/`) are checked weekly on Monday. Compatible patch/minor updates are grouped per ecosystem; major updates always arrive as individual PRs. When `packages/cline-sdk-bridge` gains its D3 npm manifest, add a dedicated dependency-update entry for that package as specified by the Cline compatibility policy below.
- GitHub Actions are pinned to full commit SHAs with a same-line `# vX` version comment. Dependabot version updates DO update `owner/action@<commit>` references, including that adjacent same-line comment. Dependabot vulnerability alerts and alert-driven security updates are NOT generated for actions pinned to SHA values (alerting requires semantic version references), so the weekly version-update entry is the automated refresh channel for them.
- The Cline bridge pins the D1-selected `@cline/sdk` version once D3 introduces its manifest. Dependabot must surface newer stable `@cline/sdk` releases, including `0.0.x` increments, as individual compatibility-signal PRs on the dedicated Cline cadence; they never auto-authorize, auto-merge, or silently change OpenOrc's supported Cline baseline.
- Dependabot PRs are a maintenance channel, not an authorization to float pins; dependency additions and changes still follow the root dependency policy (authoritative registry versions, pinned, lockfile-committed).

