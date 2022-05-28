from typing import Any, List, Mapping, Optional

import geopandas as gpd # type: ignore

from ..types.execution import ExecutionSummary, ExecutionKey, S3OutputArgument, Key
from ..execution import insertLocalArgument, insertHTTPArgument, insertS3InputArgument, insertKeywordArgument, insertExecutionKey, insertS3OutputArgument
from ..datasource.datasource import DataSource
from ..datasource.s3_file import S3File
from ..types.inputs import S3Type
from ..inputs import getS3TypeIdByName, getS3Type

from .wrapper_types import RawFunctionOutputs

def insertExecutionArguments(cursor : Any,
                             keys                : List[Key],
                             execution_summary   : ExecutionSummary,
                            ) -> None:
  execution_id = execution_summary.execution_id
  function_id = execution_summary.function_id
  for l in execution_summary.inputs.local:
    insertLocalArgument(cursor, l)
  for h in execution_summary.inputs.http:
    insertHTTPArgument(cursor, h)
  for s in execution_summary.inputs.s3:
    insertS3InputArgument(cursor, s)
  for ka in execution_summary.inputs.keyword:
    insertKeywordArgument(cursor, ka)
  for k in keys:
    e = ExecutionKey(
          execution_id = execution_id,
          geospatial_key_id = k.geospatial_key_id,
          temporal_key_id = k.temporal_key_id,
        )
    insertExecutionKey(cursor, e)
  for o in execution_summary.outputs.s3:
    insertS3OutputArgument(cursor, o)

  print("Wrote for: ",execution_summary.execution_id)


def insertInitFile(cursor : Any,
                   datasource : DataSource,
                   input_matrix : gpd.GeoDataFrame,
                   bucket_name  : str,
                  ) -> None:
  s3_type_id = getS3TypeIdByName(cursor, "input_geodataframe_type")
  s3_type, maybe_tagged_s3_sub_type = getS3Type(cursor, s3_type_id)
  input_s3 = S3File(input_matrix, "input")
  if isinstance(input_s3, S3File):
    tagged_s3_sub_type = maybe_tagged_s3_sub_type
    s3_file_meta = input_s3.to_file(tagged_s3_sub_type)

    s3_type_name = s3_type.type_name
    s3_object_id = datasource.upload_file(s3_type_id, bucket_name, s3_file_meta)
  else:
    raise Exception("Init file must be an S3 File")


def insertRawOutputs(cursor : Any,
                     datasource : DataSource,
                     raw_outputs : RawFunctionOutputs,
                     output_to_type_name : Mapping[str, str],
                     bucket_name : str,
                    ) -> Optional[int]:
  execution_summary = datasource.execution_summary
  function_id = execution_summary.function_id
  execution_id = execution_summary.execution_id
  for output_name, output in raw_outputs.items():
    output_type = output_to_type_name[output_name]
    s3_type_id = getS3TypeIdByName(cursor, output_type)
    s3_type, maybe_tagged_s3_sub_type = getS3Type(cursor, s3_type_id)
    if isinstance(output, S3File):
      tagged_s3_sub_type = maybe_tagged_s3_sub_type
      s3_file_meta = output.to_file(tagged_s3_sub_type)

      s3_type_name = s3_type.type_name
      s3_object_id = datasource.upload_file(s3_type_id, bucket_name, s3_file_meta)
    else:
      raise Exception(f"Bad output provided: {output_name}")
    a = S3OutputArgument(
          function_id = function_id,
          execution_id = execution_id,
          s3_output_parameter_name = output_name,
          s3_type_id = s3_type_id,
          s3_object_id = s3_object_id,
         )
    execution_summary.outputs.s3.append(a)

  insertExecutionArguments(cursor, datasource.keys, datasource.execution_summary)

  return execution_id
