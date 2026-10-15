"""Official Cline Agent Runtime adapter boundary.

The Python Cline adapter (#74) implements the universal #67
``AgentRuntimeAdapter`` contract on top of the language-neutral
``ClineSdkBackend`` seam (#71); the Node bridge (#72/#73) provides its
first real implementation, and ``tests/fakes/cline_sdk_backend.py`` mirrors
the same interface for deterministic tests. Runtime-specific capabilities
stay behind this local boundary and never expand the universal contract.
"""

from openorc.adapters.cline.activity import (
    ClineAccumulatedUsage,
    ClineActivityDetail,
    ClineActivityKind,
    ClineActivityObservation,
    ClineActivitySnapshot,
    ClineCommentaryActivity,
    ClineFinishActivity,
    ClineIterationActivity,
    ClineNoticeActivity,
    ClineNoticeCategory,
    ClineRuntimeEndedActivity,
    ClineRuntimeErrorActivity,
    ClineRuntimeStatusActivity,
    ClineToolActivity,
    ClineToolCategory,
    ClineTurnFinishReason,
    ClineUsageActivity,
    project_session_event,
    project_usage_summary,
)
from openorc.adapters.cline.adapter import ClineAdapter
from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend
from openorc.adapters.cline.classification import classify_backend_failure
from openorc.adapters.cline.connection_endpoint import (
    CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY,
    ConnectionControlEndpointError,
    parse_control_endpoint_url,
    resolve_connection_control_endpoint,
)
from openorc.adapters.cline.construction import (
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineSessionIdentity,
)
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
from openorc.adapters.cline.reconciliation import (
    ClineDispatchCheckpoint,
    ClineReconciliationResult,
    ClineSessionContinuity,
    ClineSettlementOutcome,
    ClineSettlementRefusal,
)
from openorc.adapters.cline.review_subject import ClineReviewSubjectUnverifiedError
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
    "CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY",
    "ClineAccumulatedUsage",
    "ClineAdapter",
    "ClineActivityDetail",
    "ClineActivityKind",
    "ClineActivityObservation",
    "ClineActivitySnapshot",
    "ClineBackendUncertainOutcomeError",
    "ClineBridgeProcessBackend",
    "ClineBridgeProtocolError",
    "ClineCommentaryActivity",
    "ClineConstructionContext",
    "ClineConstructionContextResolver",
    "ClineConstructionMode",
    "ClineDispatchCheckpoint",
    "ClineFinishActivity",
    "ClineIterationActivity",
    "ClineNoticeActivity",
    "ClineNoticeCategory",
    "ClineReconciliationResult",
    "ClineRemoteAttachmentRejectedError",
    "ClineRemoteAttachmentUnavailableError",
    "ClineRemoteConfig",
    "ClineReviewSubjectUnverifiedError",
    "ClineRuntimeEndedActivity",
    "ClineRuntimeErrorActivity",
    "ClineRuntimeStatusActivity",
    "ClineSdkBackend",
    "ClineSdkBackendError",
    "ClineSdkOperationRejectedError",
    "ClineSessionContinuity",
    "ClineSessionEvent",
    "ClineSessionIdentity",
    "ClineSessionNotFoundError",
    "ClineSettlementOutcome",
    "ClineSettlementRefusal",
    "ClineStartRequest",
    "ClineStartResult",
    "ClineToolActivity",
    "ClineToolCategory",
    "ClineTurnFinishReason",
    "ClineUsageActivity",
    "ConnectionControlEndpointError",
    "JsonObject",
    "Subscription",
    "classify_backend_failure",
    "parse_control_endpoint_url",
    "project_session_event",
    "project_usage_summary",
    "resolve_connection_control_endpoint",
]
