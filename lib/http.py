from typing import TypedDict, Callable, Optional, Union, Any, Text, Dict
from enum import Enum

from functools import partial

import psycopg2
import requests

from .types import HTTPType, HTTPVerb, RequestBodySchema
from .schema_api import insertHTTPType


http_verb_to_string : Callable[[HTTPVerb], str] = lambda v : v.name.lower()

# TODO: How to get geospatial info into requests using decorator
#       Decorator could probably inject some state into the function itself?
#       Datasource idea?
# TODO: How to enforce that geospatial info is provided? Or, issue warning.
def wrap_requests_fn(requests_fn : Callable[..., requests.Response], http_verb : HTTPVerb) -> Callable[..., requests.Response]:
  def wrapped(uri : Union[bytes, Text], *args, **kwargs) -> requests.Response:
    params : Optional[requests.sessions._Params] = kwargs.get("params")
    json   : Optional[Any] = kwargs.get("json")
    verb   : str = http_verb_to_string(http_verb)
    return requests_fn(uri, *args, **kwargs)

  return wrapped

get = wrap_requests_fn(requests.get, HTTPVerb.GET)
put = wrap_requests_fn(requests.put, HTTPVerb.PUT)
post = wrap_requests_fn(requests.post, HTTPVerb.POST)
delete = wrap_requests_fn(requests.delete, HTTPVerb.DELETE)

psycopg2.extensions.register_adapter(HTTPVerb, lambda v : psycopg2.extensions.AsIs("".join(["'", http_verb_to_string(v), "'"])))
psycopg2.extensions.register_adapter(RequestBodySchema, lambda d : psycopg2.extras.Json(d.schema))

# TODO: Add support for 'gql' or 'graphql-core' library

if __name__ == "__main__":
  hostname = "localhost"
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  http_get = HTTPType(
               type_name           = "foo_type",
               verb                = HTTPVerb.GET,
               uri                 = "http://localhost",
               uri_parameters      = ["field_id", "start", "end"],
               request_body_schema = None,
             )
  http_get_id = insertHTTPType(cursor, http_get)
  print("HTTP GET ID: ",http_get_id)


  http_post = HTTPType(
                type_name           = "bar_type",
                verb                = HTTPVerb.POST,
                uri                 = "http://localhost",
                uri_parameters      = None,
                request_body_schema = RequestBodySchema({"type": "object",
                                                         "properties": {
                                                             "field_id": {"type": "number"},
                                                             "start":    {"type": "string",
                                                                           "format": "date-time"
                                                                         },

                                                         }
                                                       })
              )
  http_post_id = insertHTTPType(cursor, http_post)
  print("HTTP POST ID: ",http_post_id)

  connection.commit()


