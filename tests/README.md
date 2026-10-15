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
OPENORC_CLINE_LIVE_PROVIDER='<configured provider identifier>' \
OPENORC_CLINE_LIVE_MODEL='<configured model identifier>' \
OPENORC_CLINE_LIVE_REPOSITORY_BASE='<Box-side scratch repository base>' \
OPENORC_CLINE_LIVE_REVIEWER_DIRECTORY='<Box-side neutral non-repository directory>' \
./scripts/devserver.sh --testdb -- \
  .venv/bin/python -m pytest -o addopts=--strict-markers -m integration -rP \
  tests/integration/test_cline_live_runtime.py
```

Before running:

- **Remote versions:** verify the Box's actual runtime versions with the supported Cloud `cline-box` verification flow (which checks the CLI pin and fails closed on drift) or the recorded current Box facts, then attest them in `OPENORC_CLINE_LIVE_ATTESTED_VERSIONS`. A mismatch blocks the run instead of silently changing a pin.
- **Remote directories:** the Box-side repository base and neutral Reviewer directory must exist per the Owner-operated Box setup; the test never probes remote paths from the local filesystem.
- **Local prerequisites:** `node` on PATH and a built `packages/cline-sdk-bridge/dist/` (`npm ci && npm run build` in that package); the bridge's `@cline/sdk` pin must equal the supported version.
- **Fixture PR:** the suite re-reads [`openorc/runtime-test-fixture#2`](https://github.com/openorc/runtime-test-fixture/pull/2) from the public GitHub API immediately before the run and stops if its head is not `81204ee712cc0627221d1024af3d2434e0006c0f`. Do not merge, close, or modify that PR during D7.

Environment variables:

| Variable | Meaning |
| --- | --- |
| `OPENORC_CLINE_LIVE=1` | explicit live opt-in; without it every test skips cleanly, with it a missing prerequisite is a clear preflight failure |
| `OPENORC_CLINE_LIVE_ATTESTED_VERSIONS` | Owner-attested actual Box runtime trio (required) |
| `OPENORC_CLINE_LIVE_CONTROL_ENDPOINT` | non-secret `safe_config.control_endpoint` for the test Connection |
| `OPENORC_CLINE_LIVE_HUB_TOKEN` | Hub attach credential; stored through the #55 trusted credential service (Vault), never logged |
| `OPENORC_CLINE_LIVE_PROVIDER` / `OPENORC_CLINE_LIVE_MODEL` | opaque role-binding provider/model configuration |
| `OPENORC_CLINE_LIVE_REPOSITORY_BASE` / `OPENORC_CLINE_LIVE_REVIEWER_DIRECTORY` | Box-side Producer base and neutral Reviewer directory |
| `OPENORC_CLINE_LIVE_BOX_CHECK_COMMAND` | optional Owner-approved operator-check command template (`{path}` placeholder, exit 0 = exists) used to verify the Task checkout and its cleanup on the actual Box; without it those verifications are Owner-attested evidence recorded on the PR |
| `OPENORC_CLINE_LIVE_RESTART_COMMAND` | Owner-approved controlled Hub-restart hook for scenario D3 (supplying it authorizes execution; only with no unrelated active work on the Box) |
| `OPENORC_CLINE_LIVE_PAUSE_SENTINEL` | alternative manual mode: the test pauses, the Owner restarts the Hub manually, then creates this sentinel file to resume |
| `OPENORC_CLINE_LIVE_PAUSE_TIMEOUT_SECONDS` | pause-mode wait bound (default 1800) |
| `OPENORC_CLINE_LIVE_ROTATE_CREDENTIAL=1` | the restart changed the Hub credential: the test rotates the stored secret through the trusted `rotate_connection_credential` service using the current `OPENORC_CLINE_LIVE_HUB_TOKEN` before the reattachment |
| `OPENORC_CLINE_LIVE_RESUME=1` + `OPENORC_CLINE_LIVE_HANDOFF_PATH` | two-stage preserved-database mode: stage one writes safe identifiers/counts to the handoff file and keeps the durable rows; stage two (against the same preserved database, run alone with `-k "d3 or cleanup"`) resumes the original READY binding without resetting migrations or re-admitting anything |

Behavior notes:

- Run the module whole: the scenarios share module-scoped state in file order (A attachments → B workspace lifecycle → C verified review → D1/D2 interruption & reconciliation → D3 Hub-restart recovery → E activity/usage → cleanup).
- Scenario D3 is a completion gate: with no restart mode configured it fails as `NOT EXECUTED` — distinct from a runtime `UNKNOWN` continuity classification — and the corresponding acceptance criterion stays unsatisfied pending execution or an explicit Owner-approved scope exception recorded on the PR.
- Ordinary sends are never asserted to be refused by an uncertain-outcome latch; the implemented contract refuses same-ID reconstruction and gates settlement behind positive caller attestation (deterministic D6b suites own the full matrix).
- A post-restart accumulated-usage reset is observational only; it is never session-loss evidence and never invalidates the durable binding.
- Evidence: run with `-rP` (or `-s`) to capture the `D7 LIVE EVIDENCE SUMMARY` printed at teardown — safe identifiers, attested versions, and per-scenario EXECUTED/OBSERVED classifications only. Record it on the PR. Durable test rows are removed through the Auth-root cascade at teardown unless the handoff mode preserves them; the branch database itself is deleted by `--testdb` on exit.


