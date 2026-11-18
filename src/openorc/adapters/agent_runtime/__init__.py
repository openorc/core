"""Shared runtime-neutral Agent Runtime adapter contract (issue #67).

Concrete Agent Runtime adapters implement the universal session contract
(``AgentRuntimeAdapter``); application services consume its typed results
and normalized error taxonomy. Runtime-specific capabilities stay behind
local adapter boundaries and never expand the universal contract.
"""

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import (
    AgentTurnAdmission,
    DetachedTurnRuntime,
)
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeInterruptedTurnError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentRuntimeSettlementOutcome,
    AgentRuntimeSettlementRefusal,
    AgentSessionAllocation,
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentTextResponse,
)

__all__ = [
    "AgentRuntimeAdapter",
    "AgentRuntimeConfigurationRejectedError",
    "AgentRuntimeDeliveryUncertainError",
    "AgentRuntimeError",
    "AgentRuntimeInterruptedTurnError",
    "AgentRuntimeProtocolFailureError",
    "AgentRuntimeSettlementOutcome",
    "AgentRuntimeSettlementRefusal",
    "AgentRuntimeTimeoutError",
    "AgentRuntimeTransportError",
    "AgentRuntimeUnavailableError",
    "AgentRuntimeUncertainOutcomeError",
    "AgentSessionAllocation",
    "AgentSessionCreated",
    "AgentSessionCreationRequest",
    "AgentSessionNotFoundError",
    "AgentTextResponse",
    "AgentTurnAdmission",
    "DetachedTurnRuntime",
]
