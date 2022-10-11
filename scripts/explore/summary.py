from typing import Optional, TypeVar

from dataclasses import dataclass

@dataclass(frozen=True)
class Summary:
  pass

RawRowType = TypeVar('RawRowType', bound=Summary)

