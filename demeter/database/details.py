from typing import Union, Mapping, Optional

import json

from dataclasses import dataclass


JSONAllowed = Union[int, float, str, None, 'Details']
_Details = Mapping[str, Optional[JSONAllowed]]


@dataclass(frozen=True, init=True)
class Details():
  details : _Details

  def __hash__(self) -> int:
    return hash(json.dumps(self.details, sort_keys=True))

