from typing import (
    Any,
    Callable,
    Dict,
    List,
    cast,
)

import requests

from ... import data

KeyToArgsFunction = Callable[[data.Key], Dict[str, Any]]
ResponseFunction = Callable[[requests.models.Response], List[Dict[str, Any]]]

OneToOneResponseFunction: ResponseFunction = lambda r: [cast(Dict[str, Any], r.json())]
OneToManyResponseFunction: ResponseFunction = lambda rs: [
    cast(Dict[str, Any], r) for r in rs.json()
]
