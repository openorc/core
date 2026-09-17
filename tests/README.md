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

- Integration tests **consume** the explicitly supplied non-production Supabase branch database; they **never provision** one. Provisioning, starting, and tearing down the target sits outside the test suite and outside agent responsibility: `--testdb` owns branch provisioning and cleanup, and nothing in the suite creates a database, starts a server, or creates or deletes a Supabase branch. The suite only resets the `openorc` schema and applies the committed migrations from scratch within the supplied database.
- The test process's only contract with provisioning is the `OPENORC_TEST_DATABASE_URL` environment variable; it never learns how the database was provisioned.
- The suite skips cleanly when `OPENORC_TEST_DATABASE_URL` is absent. Ordinary deterministic tests never require the variable and never touch a database.
- `scripts/devserver.sh` (including `--testdb`) is Owner-only manual tooling; agents never invoke it — to run these tests or for any other purpose — and never provision infrastructure for them. Live integration execution is an explicit Owner-controlled validation step.

