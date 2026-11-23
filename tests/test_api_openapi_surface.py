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
        "projects",
        "repositories",
        "role-bindings",
        "tasks",
        "health",
        "github-webhooks",
    } <= (tag_names)
    # The private handshake route is excluded from the schema, so its tag is
    # never published either.
    assert "internal-queue-observer" not in tag_names


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
