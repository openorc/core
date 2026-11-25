"""Tests for the public /api/v1 OpenAPI transport contract (issue #369).

The published contract inventory: the exact versioned public paths and their
non-public infrastructure counterparts, the private observer-registration
handshake staying out of the published schema, the declared Supabase
session-JWT bearer security scheme with per-operation security requirements,
stable operation IDs, public tag grouping, and representative documented
error responses. Route behavior keeps its own owning tests; this module owns
the transport-contract surface only.
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from openorc import __version__
from openorc.api.app import create_app

_ROLE_BINDING_PATH = "/api/v1/workspaces/{workspace_id}/role-bindings/{role}"
_PROFILE_WORKSPACES_PATH = "/api/v1/profile/workspaces"
_WORKSPACE_PATH = "/api/v1/workspaces/{workspace_id}"
_REVIEW_LIMIT_PATH = "/api/v1/workspaces/{workspace_id}/configuration/review-iteration-limit"
_GUIDANCE_PATH = "/api/v1/workspaces/{workspace_id}/configuration/guidance"
_BUDGET_PATH = "/api/v1/workspaces/{workspace_id}/configuration/default-task-budget"
_CONNECTIONS_PATH = "/api/v1/workspaces/{workspace_id}/connections"
_CONNECTION_PATH = "/api/v1/workspaces/{workspace_id}/connections/{connection_id}"
_CONNECTION_CREDENTIALS_PATH = (
    "/api/v1/workspaces/{workspace_id}/connections/{connection_id}/credentials"
)
_INSTALLATIONS_PATH = "/api/v1/workspaces/{workspace_id}/github/installations"
_INSTALLATION_PATH = "/api/v1/workspaces/{workspace_id}/github/installations/{installation_id}"
_PROJECTS_PATH = "/api/v1/workspaces/{workspace_id}/projects"
_REPOSITORIES_PATH = "/api/v1/workspaces/{workspace_id}/repositories"
_REPOSITORY_PATH = "/api/v1/workspaces/{workspace_id}/repositories/{repository_id}"
_ROUTE_PATH = "/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/installation-route"
_DISCUSSION_POST_PATH = (
    "/api/v1/workspaces/{workspace_id}/tasks/{task_id}"
    "/owner-gates/{owner_gate_id}/discussion-message"
)
_DISCUSSION_STATUS_PATH = (
    "/api/v1/workspaces/{workspace_id}/tasks/{task_id}"
    "/owner-gates/{owner_gate_id}/discussion-status"
)
_CONVERSATION_PATH = "/api/v1/workspaces/{workspace_id}/tasks/{task_id}/reviewer-conversation"
_USER_AUTH_STATUS_PATH = "/api/v1/profile/github-user-authorization"
_USER_AUTH_FLOWS_PATH = "/api/v1/profile/github-user-authorization/flows"
_USER_AUTH_REAUTH_PATH = "/api/v1/profile/github-user-authorization/reauthorization"
_ACCOUNT_DELETION_PATH = "/api/v1/profile/account"
_TASK_PATH = "/api/v1/workspaces/{workspace_id}/tasks/{task_id}"
_TASKS_PATH = "/api/v1/workspaces/{workspace_id}/tasks"
_TASK_HISTORY_PATH = _TASK_PATH + "/history"
_WORKSPACE_EVENTS_PATH = "/api/v1/workspaces/{workspace_id}/events"
_TASK_EVENTS_PATH = "/api/v1/workspaces/{workspace_id}/tasks/{task_id}/events"
_REPOSITORY_TASKS_PATH = "/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/tasks"
_ISSUE_TASKS_PATH = (
    "/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues/{issue_id}/tasks"
)
_PROJECT_PATH = "/api/v1/workspaces/{workspace_id}/projects/{project_id}"
_ISSUES_PATH = "/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/issues"
_ISSUE_PATH = _ISSUES_PATH + "/{issue_id}"
_WORKSPACE_REFRESH_PATH = "/api/v1/workspaces/{workspace_id}/refresh"
_REPOSITORY_REFRESH_PATH = "/api/v1/workspaces/{workspace_id}/repositories/{repository_id}/refresh"
_REFRESH_STATUS_PATH = _WORKSPACE_REFRESH_PATH + "/{refresh_id}"

_PUBLIC_V1_OPERATIONS: dict[tuple[str, str], str] = {
    (_PROFILE_WORKSPACES_PATH, "get"): "list_profile_workspaces",
    (_PROFILE_WORKSPACES_PATH, "post"): "create_profile_workspace",
    (_WORKSPACE_PATH, "get"): "get_workspace",
    (_REVIEW_LIMIT_PATH, "put"): "set_workspace_review_iteration_limit",
    (_GUIDANCE_PATH, "put"): "set_workspace_guidance",
    (_BUDGET_PATH, "put"): "set_workspace_default_task_budget",
    (_CONNECTIONS_PATH, "get"): "list_workspace_connections",
    (_CONNECTIONS_PATH, "post"): "create_workspace_connection",
    (_CONNECTION_PATH, "get"): "read_workspace_connection",
    (_CONNECTION_PATH, "put"): "replace_workspace_connection",
    (_CONNECTION_CREDENTIALS_PATH, "post"): "configure_connection_credential",
    (_CONNECTION_CREDENTIALS_PATH, "put"): "rotate_connection_credential",
    (_INSTALLATIONS_PATH, "get"): "list_workspace_installation_claims",
    (_INSTALLATIONS_PATH, "post"): "record_workspace_installation_claim",
    (_INSTALLATION_PATH, "get"): "read_workspace_installation_claim",
    (_PROJECTS_PATH, "get"): "list_workspace_projects",
    (_PROJECTS_PATH, "post"): "create_workspace_project",
    (_REPOSITORIES_PATH, "get"): "list_workspace_repositories",
    (_REPOSITORIES_PATH, "post"): "claim_workspace_repository",
    (_REPOSITORY_PATH, "get"): "read_workspace_repository",
    (_ROUTE_PATH, "put"): "bind_repository_installation_route",
    (_ROUTE_PATH, "delete"): "unbind_repository_installation_route",
    (_ROLE_BINDING_PATH, "get"): "get_workspace_role_binding",
    (_ROLE_BINDING_PATH, "put"): "replace_workspace_role_binding",
    (_DISCUSSION_POST_PATH, "post"): "send_owner_gate_discussion_message",
    (_DISCUSSION_STATUS_PATH, "get"): "get_owner_gate_discussion_status",
    (_CONVERSATION_PATH, "get"): "get_task_reviewer_conversation",
    (_USER_AUTH_STATUS_PATH, "get"): "read_profile_github_user_authorization",
    (_USER_AUTH_FLOWS_PATH, "post"): "start_github_user_authorization_flow",
    (_USER_AUTH_REAUTH_PATH, "post"): "start_github_user_reauthorization_flow",
    (_USER_AUTH_STATUS_PATH, "delete"): "revoke_profile_github_user_authorization",
    (_WORKSPACE_PATH, "delete"): "delete_workspace",
    (_CONNECTION_PATH, "delete"): "delete_workspace_connection",
    (_CONNECTION_CREDENTIALS_PATH, "delete"): "disconnect_connection_credential",
    (_PROJECT_PATH, "delete"): "delete_workspace_project",
    (_REPOSITORY_PATH, "delete"): "delete_workspace_repository",
    (_TASKS_PATH, "get"): "list_workspace_tasks",
    (_REPOSITORY_TASKS_PATH, "get"): "list_repository_tasks",
    (_ISSUE_TASKS_PATH, "get"): "list_repository_issue_tasks",
    (_TASK_PATH, "get"): "get_task",
    (_TASK_HISTORY_PATH, "get"): "get_task_history",
    (_WORKSPACE_EVENTS_PATH, "get"): "list_workspace_events",
    (_TASK_EVENTS_PATH, "get"): "list_task_events",
    (_TASK_PATH, "delete"): "purge_archived_task",
    (_ACCOUNT_DELETION_PATH, "delete"): "delete_account",
    (_ISSUES_PATH, "get"): "list_repository_github_issues",
    (_ISSUE_PATH, "get"): "get_repository_github_issue",
    (_WORKSPACE_REFRESH_PATH, "post"): "request_workspace_github_refresh",
    (_REPOSITORY_REFRESH_PATH, "post"): "request_repository_github_refresh",
    (_REFRESH_STATUS_PATH, "get"): "get_workspace_github_refresh",
}

_OPERATION_TAGS: dict[tuple[str, str], str] = {
    (_PROFILE_WORKSPACES_PATH, "get"): "workspaces",
    (_PROFILE_WORKSPACES_PATH, "post"): "workspaces",
    (_WORKSPACE_PATH, "get"): "workspaces",
    (_REVIEW_LIMIT_PATH, "put"): "workspaces",
    (_GUIDANCE_PATH, "put"): "workspaces",
    (_BUDGET_PATH, "put"): "workspaces",
    (_CONNECTIONS_PATH, "get"): "connections",
    (_CONNECTIONS_PATH, "post"): "connections",
    (_CONNECTION_PATH, "get"): "connections",
    (_CONNECTION_PATH, "put"): "connections",
    (_CONNECTION_CREDENTIALS_PATH, "post"): "connections",
    (_CONNECTION_CREDENTIALS_PATH, "put"): "connections",
    (_INSTALLATIONS_PATH, "get"): "github-installations",
    (_INSTALLATIONS_PATH, "post"): "github-installations",
    (_INSTALLATION_PATH, "get"): "github-installations",
    (_PROJECTS_PATH, "get"): "projects",
    (_PROJECTS_PATH, "post"): "projects",
    (_REPOSITORIES_PATH, "get"): "repositories",
    (_REPOSITORIES_PATH, "post"): "repositories",
    (_REPOSITORY_PATH, "get"): "repositories",
    (_ROUTE_PATH, "put"): "repositories",
    (_ROUTE_PATH, "delete"): "repositories",
    (_ROLE_BINDING_PATH, "get"): "role-bindings",
    (_ROLE_BINDING_PATH, "put"): "role-bindings",
    (_DISCUSSION_POST_PATH, "post"): "tasks",
    (_DISCUSSION_STATUS_PATH, "get"): "tasks",
    (_CONVERSATION_PATH, "get"): "tasks",
    (_USER_AUTH_STATUS_PATH, "get"): "github-user-authorization",
    (_USER_AUTH_FLOWS_PATH, "post"): "github-user-authorization",
    (_USER_AUTH_REAUTH_PATH, "post"): "github-user-authorization",
    (_USER_AUTH_STATUS_PATH, "delete"): "github-user-authorization",
    (_WORKSPACE_PATH, "delete"): "workspaces",
    (_CONNECTION_PATH, "delete"): "connections",
    (_CONNECTION_CREDENTIALS_PATH, "delete"): "connections",
    (_PROJECT_PATH, "delete"): "projects",
    (_REPOSITORY_PATH, "delete"): "repositories",
    (_TASKS_PATH, "get"): "tasks",
    (_REPOSITORY_TASKS_PATH, "get"): "tasks",
    (_ISSUE_TASKS_PATH, "get"): "tasks",
    (_TASK_PATH, "get"): "tasks",
    (_TASK_HISTORY_PATH, "get"): "tasks",
    (_WORKSPACE_EVENTS_PATH, "get"): "tasks",
    (_TASK_EVENTS_PATH, "get"): "tasks",
    (_TASK_PATH, "delete"): "tasks",
    (_ACCOUNT_DELETION_PATH, "delete"): "account",
    (_ISSUES_PATH, "get"): "github-issues",
    (_ISSUE_PATH, "get"): "github-issues",
    (_WORKSPACE_REFRESH_PATH, "post"): "github-refresh",
    (_REPOSITORY_REFRESH_PATH, "post"): "github-refresh",
    (_REFRESH_STATUS_PATH, "get"): "github-refresh",
}

_DOCUMENTED_ERROR_STATUSES: dict[tuple[str, str], set[str]] = {
    (_PROFILE_WORKSPACES_PATH, "get"): {"401", "503"},
    (_PROFILE_WORKSPACES_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_WORKSPACE_PATH, "get"): {"401", "404", "503"},
    (_REVIEW_LIMIT_PATH, "put"): {"401", "404", "409", "422", "503"},
    (_GUIDANCE_PATH, "put"): {"401", "404", "409", "422", "503"},
    (_BUDGET_PATH, "put"): {"401", "404", "409", "422", "503"},
    (_CONNECTIONS_PATH, "get"): {"401", "404", "409", "503"},
    (_CONNECTIONS_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_CONNECTION_PATH, "get"): {"401", "404", "409", "503"},
    (_CONNECTION_PATH, "put"): {"401", "404", "409", "422", "503"},
    (_CONNECTION_CREDENTIALS_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_CONNECTION_CREDENTIALS_PATH, "put"): {"401", "404", "409", "422", "503"},
    (_INSTALLATIONS_PATH, "get"): {"401", "404", "503"},
    (_INSTALLATIONS_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_INSTALLATION_PATH, "get"): {"401", "404", "503"},
    (_PROJECTS_PATH, "get"): {"401", "404", "503"},
    (_PROJECTS_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_REPOSITORIES_PATH, "get"): {"401", "404", "503"},
    (_REPOSITORIES_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_REPOSITORY_PATH, "get"): {"401", "404", "503"},
    (_ROUTE_PATH, "put"): {"401", "404", "409", "422", "503"},
    (_ROUTE_PATH, "delete"): {"401", "404", "409", "503"},
    (_ROLE_BINDING_PATH, "get"): {"401", "404", "503"},
    (_ROLE_BINDING_PATH, "put"): {"401", "404", "503"},
    (_DISCUSSION_POST_PATH, "post"): {"401", "404", "409", "503"},
    (_DISCUSSION_STATUS_PATH, "get"): {"401", "404", "503"},
    (_CONVERSATION_PATH, "get"): {"401", "404", "503"},
    (_USER_AUTH_STATUS_PATH, "get"): {"401", "503"},
    (_USER_AUTH_FLOWS_PATH, "post"): {"401", "409", "503"},
    (_USER_AUTH_REAUTH_PATH, "post"): {"401", "409", "503"},
    (_USER_AUTH_STATUS_PATH, "delete"): {"401", "404", "409", "503"},
    (_WORKSPACE_PATH, "delete"): {"401", "404", "409", "503"},
    (_CONNECTION_PATH, "delete"): {"401", "404", "409", "503"},
    (_CONNECTION_CREDENTIALS_PATH, "delete"): {"401", "404", "409", "503"},
    (_PROJECT_PATH, "delete"): {"401", "404", "409", "503"},
    (_REPOSITORY_PATH, "delete"): {"401", "404", "409", "503"},
    (_TASKS_PATH, "get"): {"401", "404", "422", "503"},
    (_REPOSITORY_TASKS_PATH, "get"): {"401", "404", "422", "503"},
    (_ISSUE_TASKS_PATH, "get"): {"401", "404", "422", "503"},
    (_TASK_PATH, "get"): {"401", "404", "503"},
    (_TASK_HISTORY_PATH, "get"): {"401", "404", "422", "503"},
    (_TASK_PATH, "delete"): {"401", "404", "409", "503"},
    (_ACCOUNT_DELETION_PATH, "delete"): {"401", "409", "503"},
    (_ISSUES_PATH, "get"): {"401", "404", "422", "503"},
    (_ISSUE_PATH, "get"): {"401", "404", "422", "503"},
    (_WORKSPACE_REFRESH_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_REPOSITORY_REFRESH_PATH, "post"): {"401", "404", "409", "422", "503"},
    (_REFRESH_STATUS_PATH, "get"): {"401", "404", "422", "503"},
}


def _openapi(settings: Any) -> dict[str, Any]:
    client = TestClient(create_app(settings))
    response = client.get("/openapi.json")
    assert response.status_code == 200
    return response.json()


def test_the_route_inventory_matches_the_public_v1_contract(settings_factory) -> None:
    schema = _openapi(settings_factory())
    paths = schema["paths"]

    # The exact versioned public operations plus the two unaffected
    # infrastructure endpoints; the private registration handshake stays
    # operational but is never advertised in the published schema.
    assert set(paths) == {path for path, _ in _PUBLIC_V1_OPERATIONS} | {
        "/healthz",
        "/api/github/webhooks",
    }
    assert "/internal/observer/registrations" not in paths
    # The GitHub user-authorization OAuth provider callback ingress (issue
    # #373) is provider/browser transport outside the versioned public
    # namespace: like the private handshake, it is never advertised in the
    # published schema.
    assert "/api/github/user-authorization/callback" not in paths


def test_the_bearer_scheme_is_declared_and_enforced_in_openapi(settings_factory) -> None:
    schema = _openapi(settings_factory())
    schemes = schema["components"]["securitySchemes"]

    assert set(schemes) == {"SupabaseSessionJwtBearer"}
    scheme = schemes["SupabaseSessionJwtBearer"]
    assert scheme["type"] == "http"
    assert scheme["scheme"] == "bearer"
    assert scheme["bearerFormat"] == "JWT"
    # Publishable/secret Supabase keys are not user JWTs.
    assert "keys are not user JWTs" in scheme["description"]

    for path, method in _PUBLIC_V1_OPERATIONS:
        operation = schema["paths"][path][method]
        assert {"SupabaseSessionJwtBearer": []} in operation["security"]

    # The infrastructure endpoints outside the versioned namespace declare no
    # bearer requirement: health is deterministic and the webhook ingress is
    # authenticated by the provider's own signature material.
    assert "security" not in schema["paths"]["/healthz"]["get"]
    assert "security" not in schema["paths"]["/api/github/webhooks"]["post"]


def test_public_operations_carry_stable_operation_ids_and_public_tags(settings_factory) -> None:
    schema = _openapi(settings_factory())
    paths = schema["paths"]

    for (path, method), operation_id in _PUBLIC_V1_OPERATIONS.items():
        operation = paths[path][method]
        assert operation["operationId"] == operation_id
        assert operation["tags"] == [_OPERATION_TAGS[(path, method)]]

    assert paths["/healthz"]["get"]["tags"] == ["health"]
    assert paths["/api/github/webhooks"]["post"]["tags"] == ["github-webhooks"]

    tag_names = {tag["name"] for tag in schema["tags"]}
    assert {
        "workspaces",
        "connections",
        "github-installations",
        "github-user-authorization",
        "projects",
        "repositories",
        "role-bindings",
        "tasks",
        "account",
        "health",
        "github-webhooks",
    } <= (tag_names)
    # The private handshake route is excluded from the schema, so its tag is
    # never published either; the same holds for the user-authorization
    # callback ingress (issue #373).
    assert "internal-queue-observer" not in tag_names
    assert "github-user-authorization-callback" not in tag_names


def test_representative_error_responses_are_documented(settings_factory) -> None:
    schema = _openapi(settings_factory())
    paths = schema["paths"]

    for (path, method), statuses in _DOCUMENTED_ERROR_STATUSES.items():
        responses = paths[path][method]["responses"]
        for status in statuses:
            documented = responses[status]
            assert documented["description"]
            example = documented["content"]["application/json"]["example"]
            assert example["detail"]


def test_the_published_budget_schema_is_exact_decimal_text(settings_factory) -> None:
    # pydantic serializes Decimal as canonical decimal text (the
    # exactness-preserving form, never a binary float), so the published
    # schema must declare a string — not the number type a binary float would
    # imply.
    schema = _openapi(settings_factory())
    components = schema["components"]["schemas"]
    budget_properties = {
        "DefaultTaskBudgetSetting": ["default_task_budget_usd"],
        "WorkspaceRead": ["default_task_budget_usd"],
        "DefaultTaskBudgetChange": [
            "previous_default_task_budget_usd",
            "new_default_task_budget_usd",
        ],
    }
    for name, properties in budget_properties.items():
        for prop in properties:
            members = components[name]["properties"][prop]["anyOf"]
            assert {member.get("type") for member in members} == {"string", "null"}


def test_the_api_metadata_describes_the_versioned_contract(settings_factory) -> None:
    schema = _openapi(settings_factory())
    info = schema["info"]

    assert info["title"] == "OpenOrc API"
    assert "/api/v1" in info["description"]
    # API package version metadata is distinct from the URL-major v1.
    assert info["version"] == __version__
