from typing import Dict, TypedDict, Any, Callable, List
from typing import cast

from ... import data
from ... import db

class DataSourceTypes(TypedDict):
  s3_type_ids    : Dict[db.TableId, str]
  local_type_ids : Dict[db.TableId, data.LocalType]
  http_type_ids  : Dict[db.TableId, str]

KeyToArgsFunction = Callable[[data.Key], Dict[str, Any]]


import requests

ResponseFunction = Callable[[requests.models.Response], List[Dict[str, Any]]]

OneToOneResponseFunction : ResponseFunction = lambda r : [cast(Dict[str, Any], r.json())]
OneToManyResponseFunction : ResponseFunction = lambda rs : [cast(Dict[str, Any], r) for r in rs.json()]

