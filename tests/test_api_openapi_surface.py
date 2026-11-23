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
    (_ROLE_BINDING_PATH, "get"): "get_workspace_role_binding",
    (_ROLE_BINDING_PATH, "put"): "replace_workspace_role_binding",
    (_DISCUSSION_POST_PATH, "post"): "send_owner_gate_discussion_message",
    (_DISCUSSION_STATUS_PATH, "get"): "get_owner_gate_discussion_status",
    (_CONVERSATION_PATH, "get"): "get_task_reviewer_conversation",
}

_OPERATION_TAGS: dict[tuple[str, str], str] = {
    (_ROLE_BINDING_PATH, "get"): "role-bindings",
    (_ROLE_BINDING_PATH, "put"): "role-bindings",
    (_DISCUSSION_POST_PATH, "post"): "tasks",
    (_DISCUSSION_STATUS_PATH, "get"): "tasks",
    (_CONVERSATION_PATH, "get"): "tasks",
}

_DOCUMENTED_ERROR_STATUSES: dict[tuple[str, str], set[str]] = {
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
    assert {"role-bindings", "tasks", "health", "github-webhooks"} <= tag_names
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


def test_the_api_metadata_describes_the_versioned_contract(settings_factory) -> None:
    schema = _openapi(settings_factory())
    info = schema["info"]

    assert info["title"] == "OpenOrc API"
    assert "/api/v1" in info["description"]
    # API package version metadata is distinct from the URL-major v1.
    assert info["version"] == __version__
