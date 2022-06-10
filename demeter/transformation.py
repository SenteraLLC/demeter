from typing import List, Any, Callable, Optional, Mapping

import psycopg2
from functools import wraps

from . import temporary
from .datasource.datasource import DataSource
from .datasource.register import DataSourceRegister
from .types.function import FunctionSignature
from .function import getLatestFunctionSignature

from .execution import insertExecution, getExistingExecutions
from .types.execution import ExecutionSummary, ExecutionKey, ExecutionOutputs, Execution, S3OutputArgument, Key

from .connections import getS3Connection, getPgConnection

from .util.register import registerFunction, makeDummyArguments
from .util.cli import parseCLIArguments
from .util.mode import getModeFromKwargs
from .util.mode import ExecutionMode
from .util.setup import getOutputTypes, getKeywordParameterTypes, createFunction
from .util.wrapper_types import OutputLoadFunction, WrappableFunction
from .util.wrapper_types import WrappedTransformation as WrappedTransformation
from .util.teardown import insertRawOutputs, insertInitFile
from .util.existing import getExistingDuplicate


# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)



def Transformation(name                : str,
                   major               : int,
                   output_to_type_name : Mapping[str, str],
                   load_fn             : OutputLoadFunction,
                   ) -> Callable[[WrappableFunction], WrappedTransformation]:
  def setup_datasource(fn : WrappableFunction) -> WrappedTransformation:
    # TODO: How to handle aws credentials and role assuming?
    (s3_connection, bucket_name) = getS3Connection()

    mlops_db_connection = getPgConnection()
    cursor = mlops_db_connection.cursor()

    function = createFunction(cursor, name, major)

    maybe_function_id : Optional[int] = None
    maybe_latest_signature : Optional[FunctionSignature] = None
    maybe_latest = getLatestFunctionSignature(cursor, function)
    if maybe_latest is not None:
      maybe_function_id, maybe_latest_signature = maybe_latest

    @wraps(fn)
    def add_datasource(*args : Any, **kwargs : Any) -> ExecutionOutputs:
      if len(args):
        raise Exception(f"All arguments must be named. Found unnamed arguments: {args}")

      mode = getModeFromKwargs(kwargs)
      outputs = ExecutionOutputs(
                  s3 = [],
                )

      output_types = getOutputTypes(cursor, output_to_type_name)
      keyword_types = getKeywordParameterTypes(fn)

      if mode == ExecutionMode.REGISTER:
        kwargs = makeDummyArguments(keyword_types)
        dummy_datasource = DataSourceRegister(cursor)
        load_fn(dummy_datasource, **kwargs)

        input_types = dummy_datasource.types
        maybe_new_minor = registerFunction(cursor, input_types, keyword_types, function, output_types, maybe_latest_signature)
        if maybe_new_minor is not None:
          new_minor = maybe_new_minor
          print(f"Registered function: {name} {major}.{new_minor}")

      else:
        if maybe_function_id is None:
          raise Exception(f"Function {name} {major} has not been registered.")

        function_id = maybe_function_id

        # TODO: We need to roll this back when there is no execution
        e = Execution(
              function_id = function_id,
            )
        execution_id = insertExecution(cursor, e)

        keys : List[Key] = []
        if mode == ExecutionMode.CLI:
          kwargs, default_cli_kwargs = parseCLIArguments(name, major, keyword_types)
          geospatial_key_file = default_cli_kwargs["geospatial_key_file"]
          temporal_key_file = default_cli_kwargs["temporal_key_file"]
          keys = list(temporary.load_keys(cursor, geospatial_key_file, temporal_key_file))
        elif mode == ExecutionMode.DAEMON:
          raise Exception("Key acquisition via daemon mode yet supported.")

        datasource : DataSource = DataSource(keys, function_id, execution_id, cursor, s3_connection, kwargs, keyword_types)

        load_fn(datasource, **kwargs)

        function_id = datasource.execution_summary.function_id
        existing_executions = getExistingExecutions(cursor, function_id)
        maybe_duplicate_execution = getExistingDuplicate(existing_executions, datasource.execution_summary)
        if maybe_duplicate_execution is not None:
          duplicate_execution = maybe_duplicate_execution

          # TODO: Test this
          outputs = duplicate_execution.outputs
          duplicate_execution_id = duplicate_execution.execution_id
          print(f"Detected matching execution #{duplicate_execution_id} for {name} {major}")

        else:
          m = datasource.getMatrix()
          insertInitFile(cursor, datasource, m, bucket_name)
          raw_outputs = fn(m, **kwargs)

          maybe_execution_id = insertRawOutputs(cursor, datasource, raw_outputs, output_to_type_name, bucket_name)

          if maybe_execution_id is not None:
            execution_id = maybe_execution_id
            print(f"Ran function {name} {major} as execution #{execution_id}")
      mlops_db_connection.commit()
      return outputs

    return add_datasource
  return setup_datasource

