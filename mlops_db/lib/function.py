from .datasource import DataSource, DataSourceStub, DataSourceTypes
from .ingest import S3File, LocalFile

from . import schema_api
from . import types

from . import temporary

from .connections import getS3Connection

from typing import List, TypedDict, Dict, Any, Callable, Tuple, Generator, Union, TypeVar, Protocol, Optional, Mapping
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

class ExecutionMode(Enum):
  DRY = 1
  CLI = 2
  RUN = 3

class ExecutionOptions(TypedDict):
  mode : ExecutionMode

def parseCLIArguments(fn, name, major) -> Dict[str, Any]:
  # Always remove first argumnet
  signature = inspect.signature(fn)
  first_argument = list(signature.parameters.keys())[0]
  # TODO: Generically dispose of first argumenrgt
  parser = argparse.ArgumentParser(f"CLI Arguments for '{name}' version {major}")
  annotations = fn.__annotations__
  blacklist = ["return", first_argument]
  for a, t in annotations.items():
    if a not in blacklist:
      parser.add_argument(f"--{a}", type=t, required=True)
  return vars(parser.parse_args())


T = TypeVar('T')
AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]
OutputLoadFunction = Callable[[DataSource, Any], gpd.GeoDataFrame]

WrappableFunctionOutputs = Mapping[str, Union[S3File, LocalFile]]

WrappableFunction = Callable[..., WrappableFunctionOutputs]

class AddDataSourceWrapper(Protocol):
  def __call__(self, datasource : DataSource, **kwargs: Any) -> Mapping[str, Any]: ...

class AddGeoDataFrameWrapper(Protocol):
    def __call__(self, **kwargs: Any) -> Mapping[str, Any]: ...

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


def makeDummyArguments(fn : WrappableFunction) -> Dict[str, Any]:
  signature = inspect.signature(fn)
  first_argument = list(signature.parameters.keys())[0]
  # TODO: Generically dispose of first argumenrgt
  annotations = fn.__annotations__
  blacklist = ["return", first_argument]
  out : Dict[str, Any] = {}
  for name, _type in annotations.items():
    if name not in blacklist:
      out[name] = {str : "",
                   int : 0,
                   float : 0.0,
                  }.get(name, None) # type: ignore
  return out


def getSignature(cursor : Any,
                 input_types : DataSourceTypes,
                 output_types : Dict[str, int],
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

  return types.FunctionSignature(
           local_inputs = [schema_api.getLocalType(cursor, i) for i in input_types["local_type_ids"]],
           http_inputs = [schema_api.getHTTPType(cursor, i) for i in input_types["http_type_ids"]],
           s3_inputs = [unpackS3Type(schema_api.getS3Type(cursor, i)) for i in input_types["s3_type_ids"]],
           s3_outputs = [unpackS3Type(schema_api.getS3Type(cursor, i)) for i in output_types.values()],
         )


def registerFunction(cursor : Any,
                     name : str,
                     major : int,
                     input_types : DataSourceTypes,
                     output_types : Dict[str, int],
                    ) -> int:
  # TODO: Handle registering function types somewhere else
  function_type = types.FunctionType(
                    function_type_name = "transformation",
                    function_subtype_name = None,
                  )
  function_type_id = schema_api.getMaybeFunctionTypeId(cursor, function_type)
  if function_type_id is None:
    return False

  now = datetime.now()

  f = types.Function(
        function_name = name,
        major = major,
        function_type_id = function_type_id,
        created = now,
        last_updated = now,
        details = {},
      )
  latest_signature = schema_api.getLatestFunctionSignature(cursor, f)
  if latest_signature is not None:
    signature = getSignature(cursor, input_types, output_types)

    diff = list(dictdiffer.diff(signature, latest_signature))
    if len(diff) > 0:
      raise Exception(f"Cannot register function '{name}' with major {major}:\n Diff: {diff}")

  function_id, minor = schema_api.insertFunction(cursor, f)

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
                                        last_updated = now,
                                        details = {},
                                      ))
  for i in input_types["http_type_ids"]:
    schema_api.insertHTTPParameter(cursor,
                                   types.HTTPParameter(
                                     function_id = function_id,
                                     http_type_id = i,
                                     last_updated = now,
                                     details = {},
                                   ))
  for output_name, i in output_types.items():
    schema_api.insertS3OutputParameter(cursor,
                                       types.S3OutputParameter(
                                         function_id = function_id,
                                         s3_output_parameter_name = output_name,
                                         s3_type_id = i,
                                         last_updated = now,
                                         details = {},
                                       ))

  return minor


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
    datasource : DataSource = DataSource(keys, cursor, s3_connection)

    # TODO: Only allow primitive values in kwargs?
    @wraps(fn)
    def add_datasource(*args : Any, **kwargs : Any) -> Mapping[str, AnyDataFrame]:
      if len(args):
        raise Exception(f"All arguments must be named. Found unnamed arguments: {args}")

      mode = getMode(kwargs)
      outputs : Mapping[str, Union[S3File, LocalFile]]= {}
      if mode == ExecutionMode.DRY:
        kwargs = makeDummyArguments(fn)
        dummy_datasource = DataSourceStub(cursor)
        load_fn(dummy_datasource, **kwargs)  # type: ignore

        output_types : Dict[str, int] = {}
        for output_type_name in output_to_type_name.values():
          s3_type_id = schema_api.getS3TypeIdByName(cursor, output_type_name)
          output_types[output_type_name] = s3_type_id

        minor_version = registerFunction(cursor, name, major, dummy_datasource.types, output_types)

      else:
        if mode == ExecutionMode.CLI:
          kwargs = parseCLIArguments(fn, name, major)

        load_fn(datasource, **kwargs) # type: ignore
        m = datasource.getMatrix()

        outputs = fn(m, **kwargs)
        for output_name, output in outputs.items():
          output_type = output_to_type_name[output_name]
          s3_type_id = schema_api.getS3TypeIdByName(cursor, output_type)
          s3_type, maybe_tagged_s3_sub_type = schema_api.getS3Type(cursor, s3_type_id)
          if isinstance(output, S3File):
            s3_file_meta = output.to_file(maybe_tagged_s3_sub_type)

            s3_type_name = s3_type["type_name"]
            datasource.upload_file(s3_type_id, temporary.BUCKET_NAME, s3_file_meta)
          else:
            raise Exception(f"Bad output provided: {output_name}")

      temporary.mlops_db_connection.commit()
      return outputs

    return add_datasource
  return setup_datasource


# Irrigation Status, Review Data for Yield Quantity, Protein Quantity
# Grower Field, Region (Country),
