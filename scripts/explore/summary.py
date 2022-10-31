from dataclasses import dataclass
from typing import Optional, TypeVar


@dataclass(frozen=True)
class Summary:
    pass


RawRowType = TypeVar("RawRowType", bound=Summary)
