from typing import Optional, Any, Callable
from functools import wraps

import psycopg2
import requests

from .types import HTTPType, HTTPVerb, RequestBodySchema
from .schema_api import getHTTPByName

http_verb_to_string : Callable[[HTTPVerb], str] = lambda v : v.name.lower()

# TODO: How to get geospatial info into requests using decorator
#       Decorator could probably inject some state into the function itself?
#       Datasource idea?
# TODO: How to enforce that geospatial info is provided? Or, issue warning.
def wrap_requests_fn(requests_fn : Callable[..., requests.Response],
                     cursor      : Any,
                    ) -> Callable[..., requests.Response]:
  wraps(requests_fn)
  def wrapped(uri : str,
              *args,
              **kwargs,
             ) -> requests.Response:
    return requests_fn(uri, *args, **kwargs)

  return wrapped

psycopg2.extensions.register_adapter(HTTPVerb, lambda v : psycopg2.extensions.AsIs("".join(["'", http_verb_to_string(v), "'"])))
psycopg2.extensions.register_adapter(RequestBodySchema, lambda d : psycopg2.extras.Json(d.schema))

# TODO: Add support for 'gql' or 'graphql-core' library
