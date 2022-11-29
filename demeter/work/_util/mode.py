from enum import Enum
from typing import Any, Dict, TypedDict


class ExecutionMode(Enum):
    REGISTER = 1
    CLI = 2
    DAEMON = 3


class ExecutionOptions(TypedDict):
    mode: ExecutionMode


def getModeFromKwargs(kwargs: Dict[str, Any]) -> ExecutionMode:
    mode = ExecutionMode.DAEMON
    maybe_mode = kwargs.get("mode")
    if maybe_mode is not None:
        mode = maybe_mode
        del kwargs["mode"]
    return mode
