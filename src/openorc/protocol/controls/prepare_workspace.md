Prepare the Task workspace for {{repository_full_name}} issue #{{issue_number}}.

OpenOrc has authorized preparation of your isolated Task checkout:

- Task: {{task_uuid}}
- Repository: {{repository_full_name}}
- Target checkout directory: {{target_directory}}

Create your Task checkout by cloning the authoritative repository {{repository_full_name}} into the exact target checkout directory above. Work only within that directory for this Task.

Do not create a feature branch, produce a plan, implement anything, or open a pull request yet. Normal planning begins separately after preparation has completed.

Never substitute a different repository or destination, and never destructively overwrite an unexpected existing path. If you cannot complete preparation safely, report failure rather than claiming success.

Respond with a canonical `workspace_result` with `operation` `"PREPARE"`: `status` `"COMPLETED"` when preparation is complete, or `status` `"FAILED"` when preparation cannot be completed safely.