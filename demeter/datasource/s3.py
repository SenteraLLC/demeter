from typing import Optional, Tuple, Any, Union, List
from typing import cast

from io import BytesIO

import botocore.exceptions
import pandas as pd
import geopandas as gpd # type: ignore

from ..types.execution import S3InputArgument
from ..types.inputs import TaggedS3SubType, S3TypeDataFrame
from ..inputs import getS3ObjectByKeys, getS3Type
from ..types.core import Key
from ..types.execution import ExecutionSummary

from .s3_file import AnyDataFrame
from .s3_file import toPandasFileType, FILETYPE_TO_PANDAS_READ_FN


def download(s3_connection : Any,
             s3_bucket_name : str,
             s3_key      : str,
            ) -> BytesIO:
  dst = BytesIO()
  try:
    s3_connection.Bucket(s3_bucket_name).download_fileobj(Key=s3_key, Fileobj=dst)
  except botocore.exceptions.ClientError:
    raise Exception(f"Failed to download S3 file {s3_key} from {s3_bucket_name}")

  dst.seek(0)
  return dst


def getRawS3(cursor     : Any,
             s3_connection : Any,
             keys       : List[Key],
             type_name  : str,
             execution_summary : ExecutionSummary,
            ) -> Tuple[BytesIO, Optional[TaggedS3SubType]]:
    maybe_id_and_object = getS3ObjectByKeys(cursor, keys, type_name)
    if maybe_id_and_object is None:
      raise Exception(f"Failed to find S3 object '{type_name}' associated with keys")
    s3_object_id, s3_object = maybe_id_and_object
    s3_type_id = s3_object.s3_type_id
    s3_type, maybe_tagged_s3_subtype = getS3Type(cursor, s3_type_id)
    s3_key = s3_object.key
    bucket_name = s3_object.bucket_name
    f = download(s3_connection, bucket_name, s3_key)

    s = S3InputArgument(
          function_id = execution_summary.function_id,
          execution_id = execution_summary.execution_id,
          s3_type_id = s3_type_id,
          s3_object_id = s3_object_id,
        )
    execution_summary.inputs.s3.append(s)

    return f, maybe_tagged_s3_subtype


def rawToDataFrame(raw_results : BytesIO,
                   dataframe_subtype : S3TypeDataFrame,
                  ) -> Tuple[Optional[pd.DataFrame], Optional[gpd.GeoDataFrame]]:
  driver = dataframe_subtype.driver
  has_geometry = dataframe_subtype.has_geometry
  if has_geometry:
    gdf = gpd.read_file(raw_results, driver=driver)
    return None, gdf
  else:
    pandas_file_type = toPandasFileType(driver)
    pandas_driver_fn = FILETYPE_TO_PANDAS_READ_FN[pandas_file_type]
    df = pandas_driver_fn(raw_results)
    return df, None


