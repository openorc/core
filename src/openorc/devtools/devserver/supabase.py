"""Ephemeral hosted Supabase preview-branch lifecycle for one invocation.

The branch created by THIS invocation is the only branch this module is
allowed to delete automatically. Credential values are resolved into
child-process environments and are never logged or written to files.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from openorc.devtools.devserver.environment import _strip_quotes
from openorc.devtools.devserver.errors import DevserverError
from openorc.devtools.devserver.logging import Logger, redact_url, sanitize_cli_error
from openorc.devtools.devserver.processes import ProcessRunner

BRANCH_NAME_PREFIX = "openorc-e2e"
BRANCH_WAIT_MAX_ATTEMPTS_ENV = "OPENORC_SUPABASE_BRANCH_WAIT_MAX_ATTEMPTS"
BRANCH_WAIT_SLEEP_SECONDS_ENV = "OPENORC_SUPABASE_BRANCH_WAIT_SLEEP_SECONDS"
BRANCH_SETTLE_SECONDS_ENV = "OPENORC_SUPABASE_BRANCH_SETTLE_SECONDS"
BRANCH_WAIT_MAX_ATTEMPTS_DEFAULT = 60
BRANCH_WAIT_SLEEP_SECONDS_DEFAULT = 5.0
BRANCH_SETTLE_SECONDS_DEFAULT = 10.0


# ---------------------------------------------------------------------------
# Supabase branch environment
#
# Pinned CLI v2.117.0 may expose modern or legacy API key names. The OpenOrc
# runtime contract (see .env.example) always requires SUPABASE_PUBLISHABLE_KEY
# and SUPABASE_SECRET_KEY:
#   - publishable: SUPABASE_PUBLISHABLE_KEY, else legacy SUPABASE_ANON_KEY;
#   - secret: SUPABASE_DEFAULT_KEY, else legacy SUPABASE_SERVICE_ROLE_KEY.
# The CLI never emits a key literally named SUPABASE_SECRET_KEY.
# ---------------------------------------------------------------------------


class _BranchFetchStatus(Enum):
    OK = "ok"
    CLI_FAILED = "cli-failed"
    CREDENTIALS_NOT_PUBLISHED = "credentials-not-published"


@dataclass(frozen=True)
class BranchEnvironment:
    """Parsed, key-normalized output of `supabase branches get -o env`."""

    api_url: str = ""
    pooler_url: str = ""
    direct_url: str = ""
    publishable_key: str = ""
    secret_key: str = ""

    @property
    def database_credentials_published(self) -> bool:
        return bool(self.pooler_url or self.direct_url)

    @property
    def database_url(self) -> str:
        """Pooler URL preferred over the direct (non-pooling) URL."""
        return self.pooler_url or self.direct_url

    @property
    def runtime_overrides(self) -> dict[str, str]:
        return {
            "SUPABASE_URL": self.api_url,
            "DATABASE_URL": self.database_url,
            "SUPABASE_PUBLISHABLE_KEY": self.publishable_key,
            "SUPABASE_SECRET_KEY": self.secret_key,
        }


def parse_branch_environment(raw: str) -> BranchEnvironment:
    """Parse `supabase branches get -o env` output, normalizing API key names."""
    values: dict[str, str] = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = _strip_quotes(value)

    return BranchEnvironment(
        api_url=values.get("SUPABASE_URL", ""),
        pooler_url=values.get("POSTGRES_URL", ""),
        direct_url=values.get("POSTGRES_URL_NON_POOLING", ""),
        publishable_key=values.get("SUPABASE_PUBLISHABLE_KEY")
        or values.get("SUPABASE_ANON_KEY", ""),
        secret_key=values.get("SUPABASE_DEFAULT_KEY")
        or values.get("SUPABASE_SERVICE_ROLE_KEY", ""),
    )


# ---------------------------------------------------------------------------
# Supabase branch lifecycle
#
# The branch created by THIS invocation is the only branch this orchestrator
# is allowed to delete automatically. Credential values are resolved into
# child-process environments and are never logged or written to files.
# ---------------------------------------------------------------------------


def _make_branch_name(env: Mapping[str, str]) -> str:
    """Unique per invocation so two devserver runs never share a database."""
    user_part = re.sub(r"[^a-z0-9-]+", "-", env.get("USER", "owner").lower()) or "owner"
    timestamp = time.strftime("%Y%m%d%H%M%S")
    return f"{BRANCH_NAME_PREFIX}-{user_part}-{timestamp}-{os.getpid()}"


class SupabaseManager:
    """Ephemeral preview-branch lifecycle for one devserver invocation."""

    _AUTH_ERROR_RE = re.compile(
        r"401|403|unauthorized|forbidden|invalid access token|invalid api key"
        r"|not logged in|access token|api key|permission"
    )

    def __init__(
        self,
        *,
        runner: ProcessRunner,
        root: Path,
        env: Mapping[str, str],
        log: Logger,
        sleep: Callable[[float], None],
        max_attempts: int,
        sleep_seconds: float,
        settle_seconds: float = BRANCH_SETTLE_SECONDS_DEFAULT,
    ) -> None:
        self._runner = runner
        self._root = root
        self._env = env
        self._log = log
        self._sleep = sleep
        self._max_attempts = max_attempts
        self._sleep_seconds = sleep_seconds
        self._settle_seconds = settle_seconds
        self._branch_name: str | None = None
        self._created = False
        self._branch_environment: BranchEnvironment | None = None

    def _parent_ref(self) -> str:
        parent_ref = self._env.get("OPENORC_SUPABASE_PROJECT_REF", "")
        if not parent_ref:
            raise DevserverError("OPENORC_SUPABASE_PROJECT_REF is required for ephemeral Supabase.")
        return parent_ref

    def _require_branch(self) -> str:
        if self._branch_name is None:
            raise DevserverError("No Supabase branch was created by this invocation.")
        return self._branch_name

    def verify_configuration(self) -> None:
        """Fail closed without a parent ref; warn when the parent is production."""
        parent_ref = self._parent_ref()
        production_ref = self._env.get("OPENORC_SUPABASE_PRODUCTION_PROJECT_REF", "")
        if production_ref and parent_ref == production_ref:
            self._log.warn("Branch parent project equals the configured production project ref.")
            self._log.warn(
                "Expected when preview branches are hosted on the production project; "
                "writes still target only the branch database."
            )

    def verify_cli_pin(self) -> None:
        """Enforce strict equality with the supabase/cli-version repository pin."""
        pin_path = self._root / "supabase" / "cli-version"
        if not pin_path.is_file():
            raise DevserverError("CLI version pin file missing: supabase/cli-version")
        pinned = "".join(pin_path.read_text(encoding="utf-8").split())
        if not pinned:
            raise DevserverError("CLI version pin file is empty: supabase/cli-version")

        result = self._runner.run(["supabase", "--version"])
        if result.returncode != 0:
            raise DevserverError(
                "supabase --version exited with a non-zero status; cannot verify the "
                "installed CLI version."
            )

        normalized = result.stdout.strip()
        if normalized.lower().startswith("supabase "):
            normalized = normalized[len("supabase ") :].strip()
        normalized = normalized.removeprefix("v")

        semver_re = r"^[0-9]+(\.[0-9]+)+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$"
        if not normalized or re.match(semver_re, normalized) is None:
            raise DevserverError(
                "Could not determine the installed Supabase CLI version from unexpected "
                f"--version output: {result.stdout.strip() or '<empty>'}."
            )
        if normalized != pinned:
            raise DevserverError(
                f"Supabase CLI version mismatch: installed {normalized}, repository pin {pinned}. "
                "Align your Supabase CLI with the repository pin (see docs/supabase-migrations.md)."
            )
        self._log.log(f"Supabase CLI version OK: {normalized}")

    @property
    def branch_name(self) -> str | None:
        return self._branch_name

    def create_branch(self) -> str:
        """Create the unique ephemeral branch; records it for later deletion."""
        name = _make_branch_name(self._env)
        parent_ref = self._parent_ref()
        self._log.log(f"Creating ephemeral Supabase branch: {name}")
        result = self._runner.run(
            ["supabase", "branches", "create", name, "--project-ref", parent_ref]
        )
        if result.returncode != 0:
            raise DevserverError(f"Supabase branch creation failed for '{name}'.")
        self._branch_name = name
        self._created = True
        return name

    def _fetch_branch_environment(self) -> tuple[_BranchFetchStatus, BranchEnvironment, str]:
        """Fetch branch env output; statuses distinguish retry from failure."""
        branch_name = self._require_branch()
        result = self._runner.run(
            [
                "supabase",
                "branches",
                "get",
                branch_name,
                "--project-ref",
                self._parent_ref(),
                "-o",
                "env",
            ]
        )
        if result.returncode != 0:
            stderr = sanitize_cli_error(result.stderr, self._env)
            return _BranchFetchStatus.CLI_FAILED, BranchEnvironment(), stderr
        branch_environment = parse_branch_environment(result.stdout)
        if not branch_environment.database_credentials_published:
            return _BranchFetchStatus.CREDENTIALS_NOT_PUBLISHED, branch_environment, ""
        return _BranchFetchStatus.OK, branch_environment, ""

    def wait_until_ready(self) -> BranchEnvironment:
        """Bounded wait for credentials publication AND a settled database.

        Creating the branch does not mean Postgres/Auth/API are immediately
        usable. Hosted branches publish credentials before their database
        host resolves, so readiness requires BOTH signals. Reachability is
        also not stability: every successful ``migration list`` probe is
        followed by a fixed settle window and a confirmation probe, and
        readiness requires the confirmation to succeed too, so a branch
        that destabilizes while settling cannot pass on one instantaneous
        connection. The production/main branch never publishes database
        credentials through the API, so it can never pass this gate.
        Authentication/authorization failures fail immediately instead of
        retrying. The whole wait stays bounded by
        ``max_attempts x (poll sleep + settle window)``.
        """
        branch_name = self._require_branch()
        self._log.log(f"Waiting for Supabase branch '{branch_name}' to become ready...")
        reason = ""
        for attempt in range(1, self._max_attempts + 1):
            status, branch_environment, stderr = self._fetch_branch_environment()
            if status is _BranchFetchStatus.OK:
                probe = self._runner.run(
                    ["supabase", "migration", "list", "--db-url", branch_environment.database_url]
                )
                if probe.returncode == 0:
                    self._log.log(
                        "Supabase branch is reachable; allowing provisioning to settle "
                        f"for {self._settle_seconds:g}s..."
                    )
                    self._sleep(self._settle_seconds)
                    confirm = self._runner.run(
                        [
                            "supabase",
                            "migration",
                            "list",
                            "--db-url",
                            branch_environment.database_url,
                        ]
                    )
                    if confirm.returncode == 0:
                        self._log.log("Supabase branch is ready.")
                        self._branch_environment = branch_environment
                        return branch_environment
                    reason = "database stopped answering during settling"
                else:
                    reason = "database not answering yet"
            elif status is _BranchFetchStatus.CREDENTIALS_NOT_PUBLISHED:
                reason = "database credentials not published yet"
            else:
                if self._AUTH_ERROR_RE.search(stderr.lower()):
                    raise DevserverError(
                        f"Supabase rejected the branch operation for '{branch_name}' "
                        f"(authentication/authorization): {stderr or '<no diagnostic>'}\n"
                        "Check that SUPABASE_ACCESS_TOKEN (or your stored CLI login) is valid "
                        "and authorized for the parent project."
                    )
                reason = "branch lookup failed transiently"

            if attempt < self._max_attempts:
                self._log.log(
                    f"Branch '{branch_name}' not ready ({reason}; attempt {attempt}/"
                    f"{self._max_attempts}); waiting {self._sleep_seconds:g}s..."
                )
                self._sleep(self._sleep_seconds)

        raise DevserverError(
            f"Supabase branch '{branch_name}' did not become ready within {self._max_attempts} "
            f"attempts x {self._sleep_seconds:g}s plus up to {self._settle_seconds:g}s "
            f"provisioning-settle time per attempt (last status: {reason}). It may still be "
            "provisioning, or it may be the production/main branch (whose database credentials "
            "are never retrievable)."
        )

    def export_runtime_credentials(self, env: MutableMapping[str, str]) -> None:
        """Export resolved branch credentials into the child-process environment.

        Values are never written to .env or any tracked/generated file.
        """
        branch_environment = self._branch_environment
        if branch_environment is None:
            raise DevserverError(
                "Supabase branch credentials were not resolved for this invocation."
            )
        if not branch_environment.api_url:
            raise DevserverError(
                "Supabase branch did not publish an API URL; refusing to start the stack "
                "against an unresolved target."
            )
        if not branch_environment.database_credentials_published:
            raise DevserverError(
                "Supabase branch did not publish database credentials; refusing to start the stack."
            )
        if not branch_environment.publishable_key:
            raise DevserverError(
                "Supabase branch API keys could not be resolved: neither SUPABASE_PUBLISHABLE_KEY "
                "nor SUPABASE_ANON_KEY was present in the branch output."
            )
        if not branch_environment.secret_key:
            raise DevserverError(
                "Supabase branch API keys could not be resolved: neither SUPABASE_DEFAULT_KEY "
                "nor SUPABASE_SERVICE_ROLE_KEY was present in the branch output."
            )
        env.update(branch_environment.runtime_overrides)
        database_host = redact_url(branch_environment.database_url)
        self._log.log(
            f"Branch credentials exported for this process tree "
            f"(API URL: {branch_environment.api_url}, database: {database_host})."
        )

    def apply_migrations(self) -> None:
        """Apply current-checkout migrations through the supported tooling.

        The migration tool owns the strict db push, production identity
        guards, main-branch refusal, and fail-hard behavior; branch
        lifecycle remains this manager's responsibility.
        """
        branch_name = self._require_branch()
        self._log.log(
            f"Applying current repository migrations to Supabase branch '{branch_name}'..."
        )
        result = self._runner.run(
            [str(self._root / "scripts" / "supabase-apply-migrations.sh"), "--branch", branch_name],
            capture=False,
        )
        if result.returncode != 0:
            raise DevserverError(
                f"Supabase migration application failed for branch '{branch_name}'; refusing to "
                "start the stack against a partially migrated target."
            )

    def apply_seed_if_present(self) -> None:
        """Apply representative non-production seed data when seed.sql exists."""
        if not (self._root / "supabase" / "seed.sql").is_file():
            self._log.log("No supabase/seed.sql present; skipping seed.")
            return
        branch_environment = self._branch_environment
        if branch_environment is None or not branch_environment.database_url:
            raise DevserverError(
                "Supabase branch database URL is not resolved; cannot apply seed data."
            )
        self._log.log("Applying Supabase development seed data...")
        # --include-seed applies the seed after the migration-history check;
        # migrations were already applied, so this is a migration no-op plus
        # the seed. Fails hard on error (migration-first policy).
        result = self._runner.run(
            [
                "supabase",
                "db",
                "push",
                "--db-url",
                branch_environment.database_url,
                "--include-seed",
            ],
            capture=False,
        )
        if result.returncode != 0:
            raise DevserverError(
                "Seed data did not apply cleanly; failing hard (migration-first policy)."
            )

    def delete_branch_if_created(self, *, keep: bool) -> None:
        """Delete only the branch created by this invocation; noisy on failure.

        Forgotten branches cost money, so deletion failure is surfaced as a
        loud warning identifying the branch requiring manual cleanup.
        """
        if not self._created or self._branch_name is None:
            return
        if keep:
            self._log.warn(
                f"Keeping Supabase branch by request (--keep-supabase): {self._branch_name}"
            )
            self._log.warn("DELETE IT MANUALLY when finished to avoid continued compute charges.")
            return
        self._log.log(f"Deleting ephemeral Supabase branch: {self._branch_name}")
        result = self._runner.run(
            [
                "supabase",
                "branches",
                "delete",
                self._branch_name,
                "--project-ref",
                self._parent_ref(),
            ],
            capture=False,
        )
        if result.returncode != 0:
            self._log.warn(f"Failed to delete Supabase branch: {self._branch_name}")
            self._log.warn("DELETE IT MANUALLY to avoid continued compute charges.")
            return
        self._created = False
