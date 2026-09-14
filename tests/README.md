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
- The `integration` marker is reserved for explicitly configured real-infrastructure suites. It is not used by the ordinary baseline, and `--strict-markers` rejects unknown markers.

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
- explicit real headless integration/E2E validation later in the implementation sequence.

