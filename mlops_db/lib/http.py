from typing import Any, List, Dict, Optional, Callable, Set
from functools import wraps

import psycopg2
import requests

from .types import HTTPType, HTTPVerb, RequestBodySchema, Key
from .schema_api import getHTTPByName

import jsonschema

ResponseFunction = Callable[[requests.models.Response], List[Dict[str, Any]]]


def checkHTTPParams(params : Dict[str, Any],
                      expected_params :  Set[str]
                     ) -> None:
  try:
    missing = set(expected_params) - set(params.keys())
    # TODO: Allow optional params?
    if len(missing):
      raise Exception(f"Missing args: {missing}")
    extraneous = set(params.keys()) - set(expected_params)
    if len(extraneous):
      raise Exception(f"Extraneous args: {extraneous}")
  except KeyError:
    pass # no params
  return


KeyToArgsFunction = Callable[[Key], Dict[str, Any]]

def parseHTTPParams(expected_params : List[str],
                     param_fn        : Optional[KeyToArgsFunction],
                     k               : Key,
                    ) -> Dict[str, Any]:
  if param_fn is not None:
    params = param_fn(k)
    checkHTTPParams(params, set(expected_params))
    return params
  else:
    raise Exception("Expecting URL params but no param function provided")


def parseRequestSchema(request_body_schema : Optional[RequestBodySchema],
                        json_fn             : Optional[KeyToArgsFunction],
                        k                   : Key,
                       ) -> Dict[str, Any]:
  if json_fn is not None:
    request_body = json_fn(k)
    validator = jsonschema.Draft7Validator(request_body_schema)
    is_valid = validator.is_valid(request_body)
    return request_body
  else:
    raise Exception("Expecting HTTP json request but no json function provided")



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
