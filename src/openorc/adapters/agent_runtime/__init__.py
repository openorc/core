"""Shared runtime-neutral Agent Runtime adapter contract (issue #67).

Concrete Agent Runtime adapters implement the universal session contract
(``AgentRuntimeAdapter``); application services consume its typed results
and normalized error taxonomy. Runtime-specific capabilities stay behind
local adapter boundaries and never expand the universal contract.
"""

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentTextResponse,
)

__all__ = [
    "AgentRuntimeAdapter",
    "AgentRuntimeConfigurationRejectedError",
    "AgentRuntimeDeliveryUncertainError",
    "AgentRuntimeError",
    "AgentRuntimeProtocolFailureError",
    "AgentRuntimeTimeoutError",
    "AgentRuntimeTransportError",
    "AgentRuntimeUnavailableError",
    "AgentRuntimeUncertainOutcomeError",
    "AgentSessionCreated",
    "AgentSessionCreationRequest",
    "AgentSessionNotFoundError",
    "AgentTextResponse",
]
