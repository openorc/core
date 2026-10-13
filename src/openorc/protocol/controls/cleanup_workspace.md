Clean up the Task workspace for {{repository_full_name}} issue #{{issue_number}}.

You have been authorized cleanup of exactly one Task checkout:

- Task: {{task_uuid}}
- Repository: {{repository_full_name}}
- Task checkout directory: {{target_directory}}

Remove only that designated Task checkout directory. Do not remove any parent directory, any unrelated checkout, or anything beyond this Task checkout. There is no broader cleanup to perform.

Cleanup is authorized, not inferred: a pull request being closed is not evidence that it was merged, and no other workflow event substitutes for this authorization. If you cannot establish that cleanup completed safely, report failure; never report completed cleanup based on intent alone.

Respond with a canonical `workspace_result` with `operation` `"CLEANUP"`: `status` `"COMPLETED"` when cleanup is complete, or `status` `"FAILED"` when safe completion cannot be established.