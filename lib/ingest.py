from io import BytesIO

import geopandas as gpd
import fiona

import uuid
import os
import tarfile


from .types import Key
from .schema_api import getS3ObjectByKey


from typing import BinaryIO, Iterator, Generic, TypeVar, Any, Optional, TypedDict

# TODO: S3 Namespace?
# TODO: How to track keys?
def download(s3_connection : Any,
             s3_bucket_name : str,
             s3_key      : str,
            ) -> BytesIO:

  dst = BytesIO()
  s3_connection.Bucket(s3_bucket_name).download_fileobj(Key=s3_key, Fileobj=dst)
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
  filename_on_disk : str
  key              : str


T = TypeVar('T')
class S3File(Generic[T]):
  # NOTE: Driver comes from 'fiona.supported_drivers'
  def __init__(self,
               type_name : str,
               v         : T,
               key       : Optional[str] = None,
              ):
    self.key = str(uuid.uuid4())
    if key is not None:
      self.key = "".join([key, "-", self.key])
    self.type_name = type_name
    self.value = v


  def to_file(self, driver : str) -> S3FileMeta:
    # TODO: Serialize DataFrame in memory
    tmp_filename = "/tmp/"+str(uuid.uuid4())
    if isinstance(self.value, gpd.GeoDataFrame):
      self.value.to_file(tmp_filename, driver=driver)
    else:
      raise Exception("Unsupported S3 file type")

    if os.path.isdir(tmp_filename):
      print("IS a directory.")
      dir_filename = tmp_filename
      tmp_filename = "/tmp/"+str(uuid.uuid4())+".tar.gz"
      with tarfile.open(tmp_filename, "w:gz") as tar:
        tar.add(dir_filename, arcname=".")
    else:
      print("Not a directory.")
    print("Wrote file to: ",tmp_filename)

    return S3FileMeta(
             filename_on_disk = tmp_filename,
             key = self.key,
           )


class LocalFile(object):
  def __init__(self,
               local_type_id  : str,
               value          : float,
               local_group_id : Optional[int],
              ):
    self.type_id = local_type_id
    self.value = value
    self.local_group_id = local_group_id
