# OpenOrc Tests

The test suite will grow alongside implementation. Ordinary tests are expected to be deterministic and runnable without live external infrastructure. Fake Agent Runtime and external-system boundaries are first-class tools for exercising workflow behavior.

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

Exact commands, fixtures, markers, and local integration setup will be documented here as the tooling is introduced. Do not invent commands before the repository actually provides them.
