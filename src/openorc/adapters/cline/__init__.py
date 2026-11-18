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
    aggregate_usage_cost_usd,
    project_qualifying_pulse,
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
from openorc.adapters.cline.hub_observation import (
    OPENORC_CLIENT_IDENTITY,
    ClineHubTurnObservationSource,
)
from openorc.adapters.cline.hub_projection import (
    HubRunClassificationError,
    classify_hub_run_start,
    classify_hub_run_statuses,
    classify_terminal_hub_run,
    hub_evidence_family,
)
from openorc.adapters.cline.reconciliation import (
    ClineDispatchCheckpoint,
    ClineReconciliationResult,
    ClineSessionContinuity,
    ClineSettlementOutcome,
    ClineSettlementRefusal,
)
from openorc.adapters.cline.values import (
    ClineConstructionMode,
    ClineHubEvent,
    ClineRemoteConfig,
    ClineRunAdmission,
    ClineSessionEvent,
    ClineStartRequest,
    ClineStartResult,
    JsonObject,
    Subscription,
)

__all__ = [
    "CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY",
    "aggregate_usage_cost_usd",
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
    "ClineHubEvent",
    "ClineHubTurnObservationSource",
    "ClineIterationActivity",
    "ClineNoticeActivity",
    "ClineNoticeCategory",
    "ClineReconciliationResult",
    "ClineRemoteAttachmentRejectedError",
    "ClineRemoteAttachmentUnavailableError",
    "ClineRemoteConfig",
    "ClineRunAdmission",
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
    "HubRunClassificationError",
    "JsonObject",
    "OPENORC_CLIENT_IDENTITY",
    "Subscription",
    "classify_backend_failure",
    "parse_control_endpoint_url",
    "classify_hub_run_start",
    "classify_hub_run_statuses",
    "classify_terminal_hub_run",
    "hub_evidence_family",
    "project_qualifying_pulse",
    "project_session_event",
    "classify_terminal_hub_run",
    "project_usage_summary",
    "resolve_connection_control_endpoint",
]
