from typing import Dict, TypedDict, Any, Callable, List
from typing import cast

import requests

from ..types.local import LocalType
from ..types.core import Key


class DataSourceTypes(TypedDict):
  s3_type_ids    : Dict[int, str]
  local_type_ids : Dict[int, LocalType]
  http_type_ids  : Dict[int, str]

ResponseFunction = Callable[[requests.models.Response], List[Dict[str, Any]]]

OneToOneResponseFunction : ResponseFunction = lambda r : [cast(Dict[str, Any], r.json())]
OneToManyResponseFunction : ResponseFunction = lambda rs : [cast(Dict[str, Any], r) for r in rs.json()]

KeyToArgsFunction = Callable[[Key], Dict[str, Any]]

