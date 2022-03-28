
import jsonschema

from typing import Any, List, Dict, Optional, Callable, Set

from .types import Key, RequestBodySchema


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

