Use these schemas for formal responses in this session.

Planning response → `plan_result`:

{{PLAN_RESULT_SCHEMA}}

Implementation or PR-remediation completion → `implementation_result`:

{{IMPLEMENTATION_RESULT_SCHEMA}}

PR composition response → `pr_result`:

{{PR_RESULT_SCHEMA}}

Workspace lifecycle response → `workspace_result` (only when OpenOrc explicitly requests Task workspace preparation or cleanup, as a distinct authorized interaction after readiness; the `session_ready` confirmation below remains only the initial protocol-readiness handshake and never means preparation is complete):

{{WORKSPACE_RESULT_SCHEMA}}

Confirm receipt by responding with a `session_ready` response matching:

{{SESSION_READY_SCHEMA}}
