"""Official Cline Agent Runtime adapter boundary.

The Python Cline adapter (#74) implements the universal #67
``AgentRuntimeAdapter`` contract on top of the language-neutral
``ClineSdkBackend`` seam (#71); the Node bridge (#72/#73) provides its
first real implementation, and ``tests/fakes/cline_sdk_backend.py`` mirrors
the same interface for deterministic tests. Runtime-specific capabilities
stay behind this local boundary and never expand the universal contract.
"""

from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend
from openorc.adapters.cline.contract import ClineSdkBackend
from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProtocolError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineSdkBackendError,
    ClineSdkOperationRejectedError,
    ClineSessionNotFoundError,
)
from openorc.adapters.cline.values import (
    ClineConstructionMode,
    ClineRemoteConfig,
    ClineSessionEvent,
    ClineStartRequest,
    ClineStartResult,
    JsonObject,
    Subscription,
)

__all__ = [
    "ClineBackendUncertainOutcomeError",
    "ClineBridgeProcessBackend",
    "ClineBridgeProtocolError",
    "ClineConstructionMode",
    "ClineRemoteAttachmentRejectedError",
    "ClineRemoteAttachmentUnavailableError",
    "ClineRemoteConfig",
    "ClineSdkBackend",
    "ClineSdkBackendError",
    "ClineSdkOperationRejectedError",
    "ClineSessionEvent",
    "ClineSessionNotFoundError",
    "ClineStartRequest",
    "ClineStartResult",
    "JsonObject",
    "Subscription",
]
