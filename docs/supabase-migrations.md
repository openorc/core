# Supabase migrations: repository-native developer workflow

This document is the supported command reference for authoring and applying
OpenOrc schema migrations. The **policy** behind it (migration-first, no ad
hoc live-schema mutation, least-privilege grants, environment-driven project
identity) is authoritative in [`supabase/AGENTS.md`](../supabase/AGENTS.md);
this page describes how to execute it.

## Summary

| Activity | Supported command |
| --- | --- |
| Create a migration | `supabase migration new <name>` |
| Validate against a target (no writes) | `scripts/supabase-apply-migrations.sh --dry-run …` |
| Apply current-checkout migrations to a non-production target | `scripts/supabase-apply-migrations.sh …` |

Schema changes are authored in `supabase/migrations/` first, reviewed with the
code change, and only then applied to deployed environments. Production
application belongs to private `openorc/cloud`; this repository's tooling
never writes to production.

## Supabase CLI version pin (exact)

The repository commits an **exact** supported Supabase CLI version in
`supabase/cli-version` (currently `2.117.0`, the upstream stable release at
pin time; see the dependency policy in the root `AGENTS.md`). The CLI is not
version-floored: `scripts/supabase-apply-migrations.sh` refuses to run unless
`supabase --version` matches the pin exactly.

Install or align the pinned version:

```bash
npm install --global supabase@2.117.0   # exact version via npm
# or, when the Homebrew tap carries the pinned version:
brew install supabase/tap/supabase      # verify with supabase --version
```

Upgrade procedure: when a new stable release is adopted, update
`supabase/cli-version` in the same change, verify the tooling against it, and
commit the bump — never run the tooling with an unverified CLI version.

## Creating a migration

```bash
supabase migration new <short_name>
# creates supabase/migrations/<timestamp>_<short_name>.sql
```

Authoring rules that apply to every migration (see `supabase/AGENTS.md`):

- Public-schema changes explicitly define least-privilege grants; do not rely
  on broad defaults.
- Never hardcode environment/project identifiers or credentials in migration
  files.
- Migration files are committed together with the code change that depends on
  them.

## Applying migrations to a non-production target

`scripts/supabase-apply-migrations.sh` applies the **current checkout's**
`supabase/migrations/*` to exactly one explicitly designated non-production
target. It never mutates production, and it fails closed whenever a target
cannot be reliably proven non-production.

```bash
# Show what would be applied, without writing anything:
scripts/supabase-apply-migrations.sh --dry-run --branch my-branch

# Apply to a preview branch hosted under the parent project:
scripts/supabase-apply-migrations.sh --branch my-branch

# Apply to a local (loopback) Supabase stack database:
scripts/supabase-apply-migrations.sh --db-url postgresql://postgres:...@127.0.0.1:54322/postgres
```

### Target modes and the production guard

- `--branch NAME` resolves the branch through the CLI (machine-readable
  `-o env` output) against the parent project `OPENORC_SUPABASE_PROJECT_REF`,
  waits (bounded) for the branch to publish its database credentials, then
  checks the branch's **own** project identity against the configured
  production identity before any write. Branch `main` is refused outright:
  it is the production branch identity, and its database credentials are
  never retrievable anyway.
- `--db-url URL` is accepted only when the target is provably
  non-production: a loopback host (local stack), or a Supabase-hosted URL
  whose embedded project ref differs from
  `OPENORC_SUPABASE_PRODUCTION_PROJECT_REF`. If the guard is unconfigured or
  the ref cannot be reliably extracted from a non-loopback URL, the script
  fails closed. There is no override flag.

The parent project for branch operations may legitimately be the hosted
production project (preview branches are hosted on it); only read-only
branch resolution touches the parent, and that case logs a warning. Writes
always target the branch database itself.

## Environment contract

Variables already documented in `.env.example` (copy to `.env`, never
committed; exported variables win over `.env`):

| Variable | Used for |
| --- | --- |
| `OPENORC_SUPABASE_PROJECT_REF` | Parent project ref for `--branch` mode |
| `OPENORC_SUPABASE_PRODUCTION_PROJECT_REF` | Production identity guard (required for non-loopback `--db-url` targets) |
| `SUPABASE_ACCESS_TOKEN` | CLI authentication for `--branch` mode |

Generated branch credentials (database URLs, API keys) are consumed in-process
by the tooling and are never printed, written to tracked files, or committed.

## Behavior notes

- The script enforces the exact CLI pin before anything else, refuses
  ambiguous or missing targets, and never uses `--include-all` (history
  drift must fail hard, not be masked).
- An empty local `supabase/migrations/` is a clean no-op ("nothing to
  apply") rather than an error.
- A failed push fails the script hard; partial application must be
  reconciled through new migrations, not manual patching.
- URLs are logged redacted (host only); credentials are never logged.

## Development lifecycle

1. Create a git branch for the feature work.
2. Author migrations with `supabase migration new`.
3. Validate against an ephemeral Supabase branch (or a local stack database)
   with the script above.
4. Commit migrations together with the dependent code change; open the PR.
5. Deployment to production happens through `openorc/cloud`'s supported
   deployment path after merge — never first, never from this repository.

`scripts/devserver.sh` (issue #10) will create ephemeral preview branches,
invoke this tooling, export branch credentials for the local stack, and
delete the branch on exit. Branch creation/deletion is not this script's
job.

`supabase/seed.sql` is reserved for future representative non-production
seed data. The tooling skips seeding while the file is absent; do not invent
product seed data to prove tooling works.

## Manual smoke checklist (requires a hosted parent project)

The automated tests exercise the tooling against a stub CLI and never touch
live infrastructure. The full lifecycle below requires a real parent project
and access token; it is the Owner's manual verification step and is fully
exercised by `scripts/devserver.sh` E2E once issue #10 lands:

1. `supabase login` (or export `SUPABASE_ACCESS_TOKEN`).
2. Export `OPENORC_SUPABASE_PROJECT_REF` and
   `OPENORC_SUPABASE_PRODUCTION_PROJECT_REF` in your environment or `.env`.
3. Create a branch:
   `supabase branches create <name> --project-ref $OPENORC_SUPABASE_PROJECT_REF`
4. `scripts/supabase-apply-migrations.sh --dry-run --branch <name>` — expect
   a clean dry run.
5. `scripts/supabase-apply-migrations.sh --branch <name>`.
6. Delete the branch:
   `supabase branches delete <name> --project-ref $OPENORC_SUPABASE_PROJECT_REF`
7. Negative checks: `--branch main` must refuse; parent ref equal to
   `OPENORC_SUPABASE_PRODUCTION_PROJECT_REF` must warn but proceed; a remote
   `--db-url` with the production guard unset must fail closed.

## Out of scope for this repository tooling

- OpenOrc domain/persistence schema (Phase 1 work, not this tooling).
- Production migration deployment automation and production provisioning
  (private `openorc/cloud`).
- Supabase Auth application behavior.