from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import jsonschema
import requests

from ... import (
    data,
    db,
    task,
)
from .._types import ExecutionSummary, HTTPArgument
from ._response import (
    KeyToArgsFunction,
    OneToOneResponseFunction,
    ResponseFunction,
)


def checkHTTPParams(params: Mapping[str, Any], expected_params: Set[str]) -> None:
    try:
        missing = set(expected_params) - set(params.keys())
        if len(missing):
            raise Exception(f"Missing args: {missing}")
        extraneous = set(params.keys()) - set(expected_params)
        if len(extraneous):
            raise Exception(f"Extraneous args: {extraneous}")
    except KeyError:
        pass  # no params
    return


def parseHTTPParams(
    expected_params: Sequence[str],
    param_fn: Optional[KeyToArgsFunction],
    k: data.Key,
) -> Mapping[str, Any]:
    if param_fn is not None:
        params = param_fn(k)
        checkHTTPParams(params, set(expected_params))
        return params
    else:
        raise Exception("Expecting URL params but no param function provided")


def parseRequestSchema(
    request_body_schema: db.JSON,
    json_fn: Optional[KeyToArgsFunction],
    k: data.Key,
) -> Mapping[str, Any]:
    if json_fn is not None:
        request_body = json_fn(k)
        validator = jsonschema.Draft7Validator(request_body_schema)
        is_valid = validator.is_valid(request_body)
        assert is_valid, "Must be valid."
        return request_body
    else:
        raise Exception("Expecting HTTP json request but no json function provided")


# NOTE: Wait for Python 3.11 ParamSpec
def wrap_requests_fn(
    requests_fn: Callable[..., requests.Response],
    cursor: Any,
) -> Callable[..., requests.Response]:
    wraps(requests_fn)

    def wrapped(
        uri: str,
        *args: Any,
        **kwargs: Any,
    ) -> requests.Response:
        return requests_fn(uri, *args, **kwargs)

    return wrapped


# TODO: Handle responses besides JSON
# TODO: Bulk requests with related keys
def _getHTTPRows(
    cursor: Any,
    keys: List[data.Key],
    http_type: task.HTTPType,
    param_fn: Optional[KeyToArgsFunction],
    json_fn: Optional[KeyToArgsFunction],
    response_fn: ResponseFunction,
    http_options: Dict[str, Any] = {},
) -> List[Tuple[data.Key, Mapping[str, Any]]]:
    from ...task import HTTPVerb

    verb = http_type.verb
    verb_to_fn: Mapping[HTTPVerb, Callable[..., requests.Response]] = {
        HTTPVerb.GET: requests.get,
        HTTPVerb.POST: requests.post,
        HTTPVerb.PUT: requests.put,
        HTTPVerb.DELETE: requests.delete,
    }
    func = verb_to_fn[verb]
    uri = http_type.uri

    responses: List[Tuple[data.Key, Mapping[str, Any]]] = []
    for k in keys:
        expected_params = http_type.uri_parameters
        if expected_params is not None:
            http_options["params"] = parseHTTPParams(expected_params, param_fn, k)

        request_body_schema = http_type.request_body_schema
        if request_body_schema is not None:
            http_options["json"] = parseRequestSchema(request_body_schema, json_fn, k)

        wrapped = wrap_requests_fn(func, cursor)
        raw_response = wrapped(uri, **http_options)
        response_rows = response_fn(raw_response)

        for row in response_rows:
            responses.append((k, row))

    return responses


def getHTTPRows(
    cursor: Any,
    keys: List[data.Key],
    execution_summary: ExecutionSummary,
    type_name: str,
    param_fn: Optional[KeyToArgsFunction] = None,
    json_fn: Optional[KeyToArgsFunction] = None,
    response_fn: ResponseFunction = OneToOneResponseFunction,
    http_options: Dict[str, Any] = {},
) -> List[Tuple[data.Key, Mapping[str, Any]]]:
    http_type_id, http_type = task.getHTTPTypeByName(cursor, type_name)
    http_result = _getHTTPRows(
        cursor, keys, http_type, param_fn, json_fn, response_fn, http_options
    )
    h = HTTPArgument(
        function_id=execution_summary.function_id,
        execution_id=execution_summary.execution_id,
        http_type_id=http_type_id,
        number_of_observations=len(http_result),
    )
    execution_summary.inputs["http"].append(h)

    return http_result
