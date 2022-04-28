from .datasource import DataSource, DataSourceStub, DataSourceTypes
from .ingest import S3File, LocalFile

from . import schema_api
from . import types

from . import temporary

from .connections import getS3Connection

from typing import List, TypedDict, Dict, Any, Callable, Tuple, Generator, Union, TypeVar, Protocol, Optional, Mapping, Type, Sequence
from typing import cast
import psycopg2
import pandas as pd
# TODO: Write a stub for GeoDataFrame
import geopandas as gpd # type: ignore
import dictdiffer # type: ignore

import argparse
from enum import Enum
import inspect
from datetime import datetime

from functools import wraps

# TODO: Move to constants file
NOW = datetime.now()

class ExecutionMode(Enum):
  DRY = 1
  CLI = 2
  RUN = 3

class ExecutionOptions(TypedDict):
  mode : ExecutionMode

WrappableFunctionOutputs = Mapping[str, Union[S3File, LocalFile]]

WrappableFunction = Callable[..., WrappableFunctionOutputs]


def getKeywordParameterTypes(fn : WrappableFunction) -> Dict[str, Type]:
  signature = inspect.signature(fn)
  first_argument = list(signature.parameters.keys())[0]
  # Always remove first argumnet
  annotations = fn.__annotations__
  blacklist = ["return", first_argument]
  keyword_types : Dict[str, Type] = {}
  for parameter_name, _type in annotations.items():
    if parameter_name not in blacklist:
      keyword_types[parameter_name] = _type
  return keyword_types


def parseCLIArguments(name : str, major : int, keyword_types : Dict[str, Type]) -> Dict[str, Any]:
  parser = argparse.ArgumentParser(f"CLI Arguments for '{name}' version {major}")
  for parameter_name, _type in keyword_types.items():
    parser.add_argument(f"--{parameter_name}", type=_type, required=True)
  return vars(parser.parse_args())


T = TypeVar('T')
AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]
OutputLoadFunction = Callable[[DataSource, Any], gpd.GeoDataFrame]

class AddDataSourceWrapper(Protocol):
  def __call__(self, datasource : DataSource, **kwargs: Any) -> Mapping[str, Any]: ...

class AddGeoDataFrameWrapper(Protocol):
    def __call__(self, **kwargs: Any) -> types.ExecutionOutputs: ...

# TODO: Add some deferral objects (like shared ptrs) that allow some modification of values in Load section

# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)

def getMode(kwargs : Dict[str, Any]) -> ExecutionMode:
  mode = ExecutionMode.RUN
  maybe_mode = kwargs.get("mode")
  if maybe_mode is not None:
    mode = maybe_mode
    del kwargs["mode"]
  return mode


def makeDummyArguments(keyword_types : Dict[str, Type]) -> Dict[str, Any]:
  out : Dict[str, Any] = {}
  for name, _type in keyword_types.items():
    out[name] = {str : "",
                 int : 0,
                 float : 0.0,
                }.get(_type, None) # type: ignore
  return out

def keyword_type_to_enum(_type : Type) -> types.KeywordType:
  return {str : types.KeywordType.STRING,
          int : types.KeywordType.INTEGER,
          float : types.KeywordType.FLOAT
         }.get(_type, types.KeywordType.JSON)

def getSignature(cursor : Any,
                 function : types.Function,
                 input_types : DataSourceTypes,
                 keyword_types : Dict[str, Type],
                 output_types : Dict[str, Tuple[str, int]],
                ) -> types.FunctionSignature:
  def unpackS3Type(s3_type_and_subtype : Tuple[types.S3Type, Optional[types.TaggedS3SubType]]) -> types.S3TypeSignature:
    s3_type, maybe_tagged_s3_subtype = s3_type_and_subtype
    if maybe_tagged_s3_subtype is not None:
      tagged_s3_subtype = maybe_tagged_s3_subtype
      tag = tagged_s3_subtype["tag"]
      s3_subtype = tagged_s3_subtype["value"]
      if (tag == types.S3TypeDataFrame):
        s3_type_dataframe = cast(types.S3TypeDataFrame, s3_subtype)
        return s3_type, s3_type_dataframe
      else:
        raise Exception(f"Unhandled write for S3 sub-type: {tag} -> {s3_subtype}")
    return s3_type, None

  keyword_inputs = [types.Keyword(
                      keyword_name = keyword_name,
                      keyword_type = keyword_type_to_enum(keyword_type),
                    ) for keyword_name, keyword_type in keyword_types.items()
                   ]
  s3_inputs = [unpackS3Type(schema_api.getS3Type(cursor, i)) for i in input_types["s3_type_ids"]]
  s3_outputs = [unpackS3Type(schema_api.getS3Type(cursor, s3_type_id)) for (_, s3_type_id) in output_types.values()]

  return types.FunctionSignature(
           name = function["function_name"],
           major = function["major"],
           local_inputs = [schema_api.getLocalType(cursor, i) for i in input_types["local_type_ids"]],
           keyword_inputs = keyword_inputs,
           http_inputs = [schema_api.getHTTPType(cursor, i) for i in input_types["http_type_ids"]],
           s3_inputs = s3_inputs,
           s3_outputs = s3_outputs,
         )

# TODO: Move to argument related module
psycopg2.extensions.register_adapter(types.KeywordType, lambda v : psycopg2.extensions.AsIs("".join(["'", v.name, "'"])))

def createFunction(cursor : Any,
                   name   : str,
                   major  : int,
                  ) -> types.Function:
  # TODO: Handle registering function types somewhere else
  function_type = types.FunctionType(
                    function_type_name = "transformation",
                    function_subtype_name = None,
                  )
  function_type_id = schema_api.getMaybeFunctionTypeId(cursor, function_type)
  if function_type_id is None:
    raise Exception(f"Function type does not exist: {function_type}")

  f = types.Function(
        function_name = name,
        major = major,
        function_type_id = function_type_id,
        created = NOW,
        last_updated = NOW,
        details = {},
      )
  return f


def diffSignatures(cursor : Any,
                   candidate_signature : types.FunctionSignature,
                   maybe_latest_signature : Optional[types.FunctionSignature],
                   function : types.Function,
                  ) -> bool:
  if maybe_latest_signature is not None:
    latest_signature = maybe_latest_signature

    diff = list(dictdiffer.diff(candidate_signature, latest_signature))
    if len(diff) > 0:
      name = function["function_name"]
      major = function["major"]
      raise Exception(f"Cannot register function '{name}' with major {major}:\n Diff: {diff}")
  return True


def registerFunction(cursor : Any,
                     function : types.Function,
                     input_types : DataSourceTypes,
                     keyword_types : Dict[str, Type],
                     output_types : Dict[str, Tuple[str, int]],
                    ) -> Tuple[int, int]:
  function_id, minor = schema_api.insertFunction(cursor, function)

  for i in input_types["local_type_ids"]:
    schema_api.insertLocalParameter(cursor,
                                    types.LocalParameter(
                                      function_id = function_id,
                                      local_type_id = i,
                                   ))
  for i in input_types["s3_type_ids"]:
    schema_api.insertS3InputParameter(cursor,
                                      types.S3InputParameter(
                                        function_id = function_id,
                                        s3_type_id = i,
                                        last_updated = NOW,
                                        details = {},
                                      ))
  for i in input_types["http_type_ids"]:
    schema_api.insertHTTPParameter(cursor,
                                   types.HTTPParameter(
                                     function_id = function_id,
                                     http_type_id = i,
                                     last_updated = NOW,
                                     details = {},
                                   ))
  for output_name, (s3_type_name, s3_type_id) in output_types.items():
    schema_api.insertS3OutputParameter(cursor,
                                       types.S3OutputParameter(
                                         function_id = function_id,
                                         s3_output_parameter_name = output_name,
                                         s3_type_id = s3_type_id,
                                         last_updated = NOW,
                                         details = {},
                                       ))

  for keyword_name, keyword_type in keyword_types.items():
    schema_api.insertKeywordParameter(cursor,
                                      types.KeywordParameter(
                                        function_id = function_id,
                                        keyword_name = keyword_name,
                                        keyword_type = keyword_type_to_enum(keyword_type),
                                      )
                                     )

  return function_id, minor


def insertExecutionArguments(cursor : Any,
                             keys                : List[types.Key],
                             execution_summary   : types.ExecutionSummary,
                             output_name_to_type : Dict[str, Tuple[int, int]],
                            ) -> None:
  import json
  execution_id = execution_summary["execution_id"]
  function_id = execution_summary["function_id"]
  for l in execution_summary["inputs"]["local"]:
    schema_api.insertLocalArgument(cursor, l)
  for h in execution_summary["inputs"]["http"]:
    schema_api.insertHTTPArgument(cursor, h)
  for s in execution_summary["inputs"]["s3"]:
    schema_api.insertS3InputArgument(cursor, s)
  for ka in execution_summary["inputs"]["keyword"]:
    schema_api.insertKeywordArgument(cursor, ka)
  for k in keys:
    e = types.ExecutionKey(
          execution_id = execution_id,
          geospatial_key_id = k["geospatial_key_id"],
          temporal_key_id = k["temporal_key_id"],
        )
    schema_api.insertExecutionKey(cursor, e)

  print("Wrote for: ",execution_summary["execution_id"])


# TODO: Add more ways to tune "matching"
#         There could be some keyword meta-arguments for tuning
#         the duplicate detection heuristics
def getExistingDuplicate(existing_executions : Sequence[types.ExecutionSummary],
                         execution_summary : types.ExecutionSummary,
                        ) -> Optional[types.ExecutionSummary]:
  blacklist = ["function_id", "execution_id"]
  def eq(left : Any,
         right : Any,
        ) -> bool:
    for left_key, left_value in left.items():
      if left_key in blacklist:
        continue
      try:
        right_value = right[left_key]
      except KeyError:
        right_value = None
      if all(type(v) == dict for v in [left_value, right_value]):
        nested_eq = eq(left_value, right_value)
        if not nested_eq:
          return nested_eq
      elif all(type(v) == list for v in [left_value, right_value]):
        if len(left_value) != len(right_value):
          return False
        sort_by = lambda d : sorted(d.items())
        left_sorted = sorted(left_value, key=sort_by)
        right_sorted = sorted(right_value, key=sort_by)
        for l, r in zip(left_sorted, right_sorted):
          nested_eq = eq(l, r)
          if not nested_eq:
            return nested_eq
      elif left_value != right_value:
        return False
    return True

  for e in existing_executions:
    inputs = e["inputs"]
    if eq(inputs, execution_summary["inputs"]):
      return e
  return None


def Function(name                : str,
             major               : int,
             output_to_type_name : Mapping[str, str],
             load_fn             : OutputLoadFunction,
            ) -> Callable[[WrappableFunction], AddGeoDataFrameWrapper]:
  def setup_datasource(fn : WrappableFunction) -> AddGeoDataFrameWrapper:
    # TODO: How to handle aws credentials and role assuming?
    s3_connection : Any             = getS3Connection(temporary.S3_ROLE_ARN)

    cursor = temporary.mlops_db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    keys : List[types.Key]  = list(temporary.load_keys(cursor))
    function = createFunction(cursor, name, major)

    maybe_latest_signature = schema_api.getLatestFunctionSignature(cursor, function)
    maybe_function_id : Optional[int] = None
    if maybe_latest_signature is not None:
      function_id, latest_signature = maybe_latest_signature
      maybe_function_id = function_id

    # TODO: Only allow primitive values in kwargs?
    @wraps(fn)
    def add_datasource(*args : Any, **kwargs : Any) -> types.ExecutionOutputs:
      if len(args):
        raise Exception(f"All arguments must be named. Found unnamed arguments: {args}")

      mode = getMode(kwargs)
      outputs : types.ExecutionOutputs = {}
      keyword_types = getKeywordParameterTypes(fn)

      output_types : Dict[str, Tuple[str, int]] = {}
      for output_name, output_type_name in output_to_type_name.items():
        s3_type_id = schema_api.getS3TypeIdByName(cursor, output_type_name)
        output_types[output_name] = (output_type_name, s3_type_id)


      if mode == ExecutionMode.DRY:

        kwargs = makeDummyArguments(keyword_types)
        dummy_datasource = DataSourceStub(cursor)
        load_fn(dummy_datasource, **kwargs)  # type: ignore

        input_types = dummy_datasource.types

        function_id, new_minor = registerFunction(cursor, function, dummy_datasource.types, keyword_types, output_types)

        candidate_signature = getSignature(cursor, function, input_types, keyword_types, output_types)
        diffSignatures(cursor, candidate_signature, latest_signature, function)

        print(f"Registered function: {name} {major}.{new_minor}")

      else:
        if maybe_function_id is None:
          raise Exception(f"Function {name} {major} has not been registered.")
        function_id = maybe_function_id

        # TODO: We need to roll this back when there is no execution
        e = types.Execution(
              function_id = function_id,
            )
        execution_id = schema_api.insertExecution(cursor, e)

        if mode == ExecutionMode.CLI:
          keyword_types = getKeywordParameterTypes(fn)
          kwargs = parseCLIArguments(name, major, keyword_types)

        datasource : DataSource = DataSource(keys, function_id, execution_id, cursor, s3_connection, kwargs, keyword_types)

        load_fn(datasource, **kwargs) # type: ignore
        m = datasource.getMatrix()

        function_id = datasource.execution_summary["function_id"]
        existing_executions = schema_api.getExistingExecutions(cursor, function_id)
        maybe_duplicate_execution = getExistingDuplicate(existing_executions, datasource.execution_summary)
        if maybe_duplicate_execution is not None:
          duplicate_execution = maybe_duplicate_execution
          print("Detected duplicate execution: ",duplicate_execution["execution_id"])
          outputs = duplicate_execution["outputs"]
        else:
          raw_outputs = fn(m, **kwargs)
          output_name_to_type : Dict[str, Tuple[int, int]] = {}
          for output_name, output in raw_outputs.items():
            output_type = output_to_type_name[output_name]
            s3_type_id = schema_api.getS3TypeIdByName(cursor, output_type)
            s3_type, maybe_tagged_s3_sub_type = schema_api.getS3Type(cursor, s3_type_id)
            if isinstance(output, S3File):
              tagged_s3_sub_type = maybe_tagged_s3_sub_type
              s3_file_meta = output.to_file(tagged_s3_sub_type)

              s3_type_name = s3_type["type_name"]
              s3_object_id = datasource.upload_file(s3_type_id, temporary.BUCKET_NAME, s3_file_meta)
              output_name_to_type[output_name] = (s3_object_id, s3_type_id)
            else:
              raise Exception(f"Bad output provided: {output_name}")
            a = types.S3OutputArgument(
                  function_id = function_id,
                  execution_id = execution_id,
                  s3_output_parameter_name = output_name,
                  s3_type_id = s3_type_id,
                  s3_object_id = s3_object_id,
                 )
            outputs["s3"].append(a)

          insertExecutionArguments(cursor, keys, datasource.execution_summary, output_name_to_type)

      temporary.mlops_db_connection.commit()
      return outputs

    return add_datasource
  return setup_datasource


# Irrigation Status, Review Data for Yield Quantity, Protein Quantity
# Grower Field, Region (Country),
