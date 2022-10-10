from typing import Optional

from dataclasses import dataclass

@dataclass(frozen=True)
class Summary:
  name : Optional[str]

