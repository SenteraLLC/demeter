from io import BytesIO

import pandas as pd
import geopandas as gpd  # type: ignore
import fiona  # type: ignore
import botocore.exceptions

import uuid
import os
import io
import tarfile
import sys

from enum import Enum

from .types import Key, TaggedS3SubType, S3SubType, S3TypeDataFrame

from typing import BinaryIO, Iterator, Generic, TypeVar, Any, Optional, TypedDict, Dict, Callable, Union
from typing import cast

AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]

# TODO: S3 Namespace?
# TODO: How to track keys?
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


def upload(s3_connection : Any,
           bucket_name : str,
           key         : str,
           blob        : BytesIO,
          ) -> bool:
  s3_connection.Bucket(bucket_name).upload_fileobj(Key=key, Fileobj=blob)
  return True


# TODO: How to handle different drivers?
#       Maybe an S3 File heirarchy?
# TODO: Driver to extension mapping?

class S3FileMeta(TypedDict):
  filename_on_disk       : str
  key                    : str


SupportedS3DataType = Union[gpd.GeoDataFrame, pd.DataFrame, BytesIO]

class PandasFileType(Enum):
  CSV = 1
  JSON = 2

def toPandasFileType(as_string : str) -> PandasFileType:
  return PandasFileType[as_string.upper()]

# TODO: Is there any way to type these mappings from pandas stubs?
PandasReadFn = Dict[PandasFileType, Callable[[BytesIO], pd.DataFrame]]
FILETYPE_TO_PANDAS_READ_FN : PandasReadFn = {PandasFileType.CSV  : pd.read_csv,
                                             PandasFileType.JSON : pd.read_json,
                                            }

PandasWriteFn = Callable[[Any], Any]

def FILETYPE_TO_PANDAS_WRITE_FN(pandas_file_type : PandasFileType, value : pd.DataFrame) -> PandasWriteFn:
  return cast(PandasWriteFn,
              {PandasFileType.CSV  : value.to_csv,
               PandasFileType.JSON : value.to_json,
              }[pandas_file_type]
             )


def _write_s3_file_to_disk(value                   : SupportedS3DataType,
                           tmp_filename            : str,
                           maybe_tagged_s3_subtype : Optional[TaggedS3SubType],
                           converter_args          : Dict[str, Any],
                          ) -> None:
  if maybe_tagged_s3_subtype is not None:
    tagged_s3_subtype = cast(AnyDataFrame, maybe_tagged_s3_subtype)
    tag = tagged_s3_subtype["tag"]
    s3_subtype = tagged_s3_subtype["value"]
    if (tag == S3TypeDataFrame):
      s3_type_dataframe = cast(S3TypeDataFrame, s3_subtype)
      value = cast(AnyDataFrame, value)
      has_geometry = s3_type_dataframe["has_geometry"]
      driver = s3_type_dataframe["driver"]
      if has_geometry:
        value.to_file(tmp_filename, driver=driver, **converter_args)
      else:
        pandas_file_type = toPandasFileType(driver)
        to_file_fn = FILETYPE_TO_PANDAS_WRITE_FN(pandas_file_type, value)
        to_file_fn(tmp_filename, **converter_args)
    else:
       raise Exception(f"Unhandled write for S3 sub-type: {tag} -> {s3_subtype}")
  elif isinstance(value, io.IOBase):
    value.write(tmp_filename)
  else:
    raise Exception(f"Unhandled write for S3 file: {value}")
  return None



class S3File(object):
  def __init__(self,
               value      : SupportedS3DataType,
               key_prefix : Optional[str] = None,
               delim      : str = "_",
              ):
    self.value = value
    self.key = str(uuid.uuid4())
    if key_prefix is not None:
      self.key = delim.join([key_prefix, self.key])


  # NOTE: Driver comes from 'fiona.supported_drivers'
  # TODO: Support files other than dataframes
  #       Make 'driver' optional
  def to_file(self,
              maybe_tagged_s3_subtype : Optional[TaggedS3SubType],
              converter_args   : Dict[str, Any] = {},
             ) -> S3FileMeta:
    # TODO: Serialize S3 file in memory?
    tmp_filename = "/tmp/"+str(uuid.uuid4())
    dataframe_has_geometry : Optional[bool] = None

    _write_s3_file_to_disk(self.value, tmp_filename, maybe_tagged_s3_subtype, converter_args)

    if os.path.isdir(tmp_filename):
      dir_filename = tmp_filename
      tmp_filename = "/tmp/"+str(uuid.uuid4())+".tar.gz"
      with tarfile.open(tmp_filename, "w:gz") as tar:
        tar.add(dir_filename, arcname=".")
    print("Wrote local file to: ",tmp_filename)

    s3_file_meta = S3FileMeta(
                     filename_on_disk = tmp_filename,
                     key = self.key,
                   )
    return s3_file_meta


class LocalFile(object):
  def __init__(self,
               type_name      : str,
               value          : float,
               local_group_id : Optional[int],
              ):
    self.type_name = type_name
    self.value = value
    self.local_group_id = local_group_id


