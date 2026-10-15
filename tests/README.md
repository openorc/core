# OpenOrc Tests

The test suite grows alongside implementation. Ordinary tests are deterministic and runnable without live external infrastructure. Fake Agent Runtime and external-system boundaries are first-class tools for exercising workflow behavior.

## Commands

```bash
# One-time environment setup (see repository README): uv sync

.venv/bin/python -m pytest                 # full deterministic suite
.venv/bin/python -m pytest tests/test_config.py   # single module
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/pyright
```

## Layout and markers

- Ordinary tests are deterministic and require no live Supabase, Valkey, GitHub, or Cline infrastructure.
- Fake clients/adapters (hand-rolled fakes or `unittest.mock` doubles) stand in for external systems at bootstrap boundaries.
- The reusable deterministic fake Agent Runtime (issue #69) lives in `tests/fakes/` and is imported from ordinary test modules as `from tests.fakes.agent_runtime import ...`. It drives the real #67 adapter contract and the shared #67 → #65 formal-validation funnel; construct a fresh adapter per test — there is no cross-test global session registry. The reusable deterministic fake Cline SDK backend (issue #71) lives alongside it as `tests/fakes/cline_sdk_backend.py` and drives the lower `ClineSdkBackend` seam the real Cline adapter will call.
- The concrete D4 bridge-process backend (`src/openorc/adapters/cline/bridge_process.py`, issue #73) is tested by `tests/test_adapters_cline_bridge_process.py`: deterministic scripted-child tests plus a small real Python-to-Node seam test over D3's `packages/cline-sdk-bridge/tests/spawn-harness.mjs`. The seam test runs whenever its prerequisites exist (`node` on PATH and the built `packages/cline-sdk-bridge/dist/`). With `OPENORC_BRIDGE_SEAM_REQUIRED=1` — set by CI's `python-checks` job, which builds the bridge before pytest — a missing prerequisite is a hard failure instead of a skip. Build first when testing locally: `cd packages/cline-sdk-bridge && npm ci && npm run build && cd ../.. && OPENORC_BRIDGE_SEAM_REQUIRED=1 .venv/bin/python -m pytest tests/test_adapters_cline_bridge_process.py`.
- The `integration` marker is reserved for explicitly configured real-infrastructure suites. It is not used by the ordinary baseline, and the default pytest configuration (`pyproject.toml` `addopts`) deselects integration-marked tests, so ordinary local and CI runs never require live infrastructure. `pytest -m integration` overrides the default explicitly, and `--strict-markers` rejects unknown markers.

Planned coverage layers include:

- domain and application-service behavior;
- persistence constraints and repositories;
- API contracts;
- RQ job/replay behavior;
- adapter contracts;
- formal agent-response protocol validation;
- Task session lifecycle/isolation;
- GitHub reconciliation;
- frontend component/state/API contracts;
- developer/infra tooling contracts (for example, the Supabase migration tooling exercised through a stub CLI, and the devserver orchestrator exercised through Python-level process-runner fakes);
- explicit real headless integration/E2E validation later in the implementation sequence.

## Integration-marked suites

`tests/integration/` holds suites that require real infrastructure. They are excluded from the ordinary deterministic baseline by the default pytest configuration and must be invoked explicitly.

The Owner-run command for these suites is the devserver's `--testdb` mode. It reuses the canonical ephemeral Supabase branch lifecycle: provision a non-production branch, apply the committed migrations, run every persistence integration suite under `tests/integration` (or an explicitly supplied command) with `OPENORC_TEST_DATABASE_URL` pointing at the branch database, and delete the branch on exit:

```bash
./scripts/devserver.sh --testdb
```

The explicit `--` form runs an arbitrary command against the same branch (for example, one integration module):

```bash
./scripts/devserver.sh --testdb -- \
  .venv/bin/python -m pytest -o addopts=--strict-markers \
  -m integration tests/integration/test_task_agent_session_persistence.py
```

Equivalently, the suite can be pointed at any explicitly supplied non-production branch database:

```bash
OPENORC_TEST_DATABASE_URL=<supplied non-production Supabase branch database URL> \
  .venv/bin/python -m pytest -m integration tests/integration
```

- Integration tests **consume** the explicitly supplied non-production Supabase branch database; they **never provision** one. Provisioning, starting, and tearing down the target sits outside the test suite and outside agent responsibility: `--testdb` owns branch provisioning and cleanup, and nothing in the suite creates a database, starts a server, or creates or deletes a Supabase branch. The shared session fixture in `tests/integration/conftest.py` resets the `openorc` schema and applies the committed migrations from scratch within the supplied database once per pytest session (running any individual integration module directly still works), and the function-scoped `conn` fixture keeps per-test transaction rollback/isolation. Because `openorc.profiles.id` references `auth.users (id) ON DELETE CASCADE` (issue #27), Profile setup in these suites also inserts the backing Auth user row inside the test transaction.
- The test process's only contract with provisioning is the `OPENORC_TEST_DATABASE_URL` environment variable; it never learns how the database was provisioned.
- The suite skips cleanly when `OPENORC_TEST_DATABASE_URL` is absent. Ordinary deterministic tests never require the variable and never touch a database.
- `scripts/devserver.sh` (including `--testdb`) is Owner-only manual tooling; agents never invoke it — to run these tests or for any other purpose — and never provision infrastructure for them. Live integration execution is an explicit Owner-controlled validation step.

### D7 live Cline runtime validation (opt-in, Owner-operated)

`tests/integration/test_cline_live_runtime.py` (issue #149) drives the assembled production Cline path — `attach_cline_runtime` with the real `ClineBridgeProcessBackend` and Node D3 bridge, the durable construction-context resolver, and the `ClineAdapter` — against a ready managed ClineBox. It is a bounded compatibility/integration check for the frozen supported trio `cline 3.0.68` / `@cline/core 0.0.90` / `@cline/sdk 0.0.90`, not a certification framework; it is never part of CI and never runs in the ordinary deterministic baseline.

Owner run command (the Box must already be READY and reachable; `devserver.sh` provisions the disposable branch database):

```bash
OPENORC_CLINE_LIVE=1 \
OPENORC_CLINE_LIVE_ATTESTED_VERSIONS='cline@3.0.68 core@0.0.90 sdk@0.0.90' \
OPENORC_CLINE_LIVE_CONTROL_ENDPOINT='<ws(s):// host:port path of the Box Hub>' \
OPENORC_CLINE_LIVE_HUB_TOKEN='<current Hub attach credential — secret; never logged>' \
OPENORC_CLINE_LIVE_PRODUCER_PROVIDER='openrouter' \
OPENORC_CLINE_LIVE_PRODUCER_MODEL='~z-ai/glm-flash-latest' \
OPENORC_CLINE_LIVE_REVIEWER_PROVIDER='cline' \
OPENORC_CLINE_LIVE_REVIEWER_MODEL='~openai/gpt-sol-latest' \
OPENORC_CLINE_LIVE_REPOSITORY_BASE='<Box-side scratch repository base>' \
OPENORC_CLINE_LIVE_REVIEWER_DIRECTORY='<Box-side neutral non-repository directory>' \
OPENORC_CLINE_LIVE_BOX_CHECK_COMMAND='<Owner-approved operator check, e.g. ssh <box> test -d {path}' \
./scripts/devserver.sh --testdb -- \
  .venv/bin/python -m pytest -o addopts=--strict-markers -m integration -rP \
  tests/integration/test_cline_live_runtime.py
```

Before running:

- **Remote versions:** verify the Box's actual runtime versions with the supported Cloud `cline-box` verification flow (which checks the CLI pin and fails closed on drift) or the recorded current Box facts, then attest them in `OPENORC_CLINE_LIVE_ATTESTED_VERSIONS`. A mismatch blocks the run instead of silently changing a pin.
- **Remote directories:** the Box-side repository base and neutral Reviewer directory must exist per the Owner-operated Box setup; the test never probes remote paths from the local filesystem.
- **Local prerequisites:** `node` on PATH and a built `packages/cline-sdk-bridge/dist/` (`npm ci && npm run build` in that package); the bridge's `@cline/sdk` pin must equal the supported version.
- **Fixture PR (dynamic subject):** the suite resolves the prepared subject from the public GitHub API at every preflight — [`openorc/runtime-test-fixture#2`](https://github.com/openorc/runtime-test-fixture/pull/2) — validating the expected PR identity (repository, number, open, non-draft), the prepared base branch (`main`) and head branch (`qual/d7-cline-live-review`) with the head repository, the fixture structure (exactly one changed file, `fixture/d7-review-subject.md`), and pinning the CURRENT head SHA for the review interaction. The head SHA is not hardcoded; a re-prepared fixture head is picked up on the next deliberate run. Head movement detected after the review turn is a stop condition, never a silent retarget. Do not merge, close, or modify that PR during D7.
- **Genuine Box-side verification (required):** `OPENORC_CLINE_LIVE_BOX_CHECK_COMMAND` is a required prerequisite. Autonomous execution cannot rely on unperformed Owner attestation: the Task checkout's existence and its cleanup are verified by actually running the Owner-approved operator check against the Box. The outcome contract is explicit — exit 0 = path exists, exit 1 = path absent, and ANY other exit status is a verification failure (a transport, authentication, or remote-execution error is never cleanup evidence); the operator check must preserve this distinction.

Environment variables:

| Variable | Meaning |
| --- | --- |
| `OPENORC_CLINE_LIVE=1` | explicit live opt-in; without it every test skips cleanly, with it a missing prerequisite is a clear preflight failure |
| `OPENORC_CLINE_LIVE_ATTESTED_VERSIONS` | Owner-attested actual Box runtime trio (required) |
| `OPENORC_CLINE_LIVE_CONTROL_ENDPOINT` | non-secret `safe_config.control_endpoint` for the test Connection |
| `OPENORC_CLINE_LIVE_HUB_TOKEN` | Hub attach credential for the INITIAL seeding; stored through the #55 trusted credential service (Vault), never logged. A restart helper cannot update this process environment after start, so the post-restart refresh never re-reads this value — it uses the credential-supplier command instead |
| `OPENORC_CLINE_LIVE_PRODUCER_PROVIDER` / `OPENORC_CLINE_LIVE_PRODUCER_MODEL` | opaque Producer role-binding provider/model configuration (Cloud dogfood defaults: `openrouter` / `~z-ai/glm-flash-latest`) |
| `OPENORC_CLINE_LIVE_REVIEWER_PROVIDER` / `OPENORC_CLINE_LIVE_REVIEWER_MODEL` | opaque Reviewer role-binding provider/model configuration (Cloud dogfood defaults: `cline` / `~openai/gpt-sol-latest`); each role's configuration is applied only to its own binding |
| `OPENORC_CLINE_LIVE_REPOSITORY_BASE` / `OPENORC_CLINE_LIVE_REVIEWER_DIRECTORY` | Box-side Producer base and neutral Reviewer directory |
| `OPENORC_CLINE_LIVE_BOX_CHECK_COMMAND` | required Owner-approved operator-check command template (`{path}` placeholder) used to genuinely verify the Task checkout and its cleanup on the actual Box. Outcome contract: exit 0 = path exists, exit 1 = path absent, ANY other exit status = verification failure (never absence/cleanup evidence); autonomous execution requires this — unperformed Owner attestation is not accepted |
| `OPENORC_CLINE_LIVE_RESTART_COMMAND` | Owner-approved controlled Hub-restart hook for scenario D3 (supplying it authorizes execution; only with no unrelated active work on the Box) |
| `OPENORC_CLINE_LIVE_PAUSE_SENTINEL` | alternative manual mode: the test pauses, the Owner restarts the Hub manually, then creates this sentinel file to resume |
| `OPENORC_CLINE_LIVE_PAUSE_TIMEOUT_SECONDS` | pause-mode wait bound (default 1800) |
| `OPENORC_CLINE_LIVE_CREDENTIAL_COMMAND` | required when `OPENORC_CLINE_LIVE_ROTATE_CREDENTIAL=1`: a command executed FRESH at refresh time whose stdout is the CURRENT Hub attach credential (a restart helper cannot update the test process's environment after start). The value is captured without echo, never logged or persisted, never passed in command arguments, and goes straight into the trusted Vault rotation service |
| `OPENORC_CLINE_LIVE_ROTATE_CREDENTIAL=1` | the restart changed the Hub credential: the test acquires the current token through the credential-supplier command and refreshes the stored secret through the test-only `_refresh_current_hub_credential` interface (the existing trusted Vault rotation service, stable `auth_reference`) before the reattachment; the future Cloud operator runner implements its own equivalent (openorc/cloud#54) |
| `OPENORC_CLINE_LIVE_RESUME=1` + `OPENORC_CLINE_LIVE_HANDOFF_PATH` | two-stage preserved-database mode: stage one writes safe identifiers/counts to the handoff file and keeps the durable rows; stage two (against the same preserved database, run alone with `-k "d3 or cleanup"`) resumes the original READY binding without resetting migrations or re-admitting anything |

Behavior notes:

- Run the module whole: the scenarios share module-scoped state in file order (A attachments → B workspace lifecycle → C verified review → D1/D2 interruption & reconciliation → D3 Hub-restart recovery → E activity/usage → cleanup).
- Scenario D3 is a completion gate: with no restart mode configured it fails as `NOT EXECUTED` — distinct from a runtime `UNKNOWN` continuity classification — and the corresponding acceptance criterion stays unsatisfied pending execution or an explicit Owner-approved scope exception recorded on the PR.
- Ordinary sends are never asserted to be refused by an uncertain-outcome latch; the implemented contract refuses same-ID reconstruction and gates settlement behind positive caller attestation (deterministic D6b suites own the full matrix).
- A post-restart accumulated-usage reset is observational only; it is never session-loss evidence and never invalidates the durable binding.
- The test-only support helpers (`tests/integration/cline_live_support.py`) carry the fixture-subject validation and head-movement rules as pure, network-free logic, pinned by `tests/test_integration_cline_live_support.py` in the ordinary deterministic suite.
- Actual live verification of this suite is performed manually by the Owner after both this suite (openorc/core#149) and the Cloud operator runner (openorc/cloud#54) are in place; nothing here runs live inference in CI or ordinary deterministic runs.
- Evidence: run with `-rP` (or `-s`) to capture the `D7 LIVE EVIDENCE SUMMARY` printed at teardown — safe identifiers, attested versions, and per-scenario EXECUTED/OBSERVED classifications only. Record it on the PR. Durable test rows are removed through the Auth-root cascade at teardown unless the handoff mode preserves them; the branch database itself is deleted by `--testdb` on exit.


