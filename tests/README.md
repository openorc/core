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

`tests/integration/` holds suites that require real infrastructure. They are excluded from the ordinary deterministic baseline by the default pytest configuration and must be invoked explicitly:

```bash
OPENORC_TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55432/postgres \
  .venv/bin/python -m pytest -m integration tests/integration/test_ownership_persistence.py
```

- `OPENORC_TEST_DATABASE_URL` must point at a **disposable** Postgres database (for example a throwaway container or a local development stack database). The session fixture resets the `openorc` schema and applies the committed migrations from scratch before the tests run.
- Ordinary deterministic tests never require this variable and never touch a database.

