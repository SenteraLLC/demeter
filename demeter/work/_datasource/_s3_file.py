import os.path
import tarfile
import uuid
from enum import Enum
from io import BufferedIOBase, BytesIO, IOBase
from typing import Any, Callable, Dict, Optional, TypedDict, Union, cast

import geopandas as gpd  # type: ignore
import pandas as pd

from ... import task

AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]

SupportedS3DataType = Union[gpd.GeoDataFrame, pd.DataFrame, BytesIO]


class S3FileMeta(TypedDict):
    filename_on_disk: str
    key: str


class S3File(object):
    def __init__(
        self,
        value: SupportedS3DataType,
        key_prefix: Optional[str] = None,
        delim: str = "_",
    ):
        self.value = value
        self.key = str(uuid.uuid4())
        if key_prefix is not None:
            self.key = delim.join([key_prefix, self.key])

    def to_file(
        self,
        maybe_tagged_s3_subtype: Optional[task.TaggedS3SubType],
        converter_args: Dict[str, Any] = {},
    ) -> S3FileMeta:
        tmp_filename = "/tmp/" + str(uuid.uuid4())
        dataframe_has_geometry: Optional[bool] = None

        # TODO: Serialize S3 file in memory?
        writeS3FileToDisk(
            self.value, tmp_filename, maybe_tagged_s3_subtype, converter_args
        )

        if os.path.isdir(tmp_filename):
            dir_filename = tmp_filename
            tmp_filename = "/tmp/" + str(uuid.uuid4()) + ".tar.gz"
            with tarfile.open(tmp_filename, "w:gz") as tar:
                tar.add(dir_filename, arcname=".")
        print("Wrote local file to: ", tmp_filename)

        s3_file_meta = S3FileMeta(
            filename_on_disk=tmp_filename,
            key=self.key,
        )
        return s3_file_meta


class PandasFileType(Enum):
    CSV = 1
    JSON = 2


def toPandasFileType(as_string: str) -> PandasFileType:
    return PandasFileType[as_string.upper()]


PandasReadFn = Dict[PandasFileType, Callable[[BufferedIOBase], pd.DataFrame]]
FILETYPE_TO_PANDAS_READ_FN: PandasReadFn = {
    PandasFileType.CSV: pd.read_csv,  # type: ignore
    PandasFileType.JSON: pd.read_json,  # type: ignore
}

PandasWriteFn = Callable[[Any], Any]


def FILETYPE_TO_PANDAS_WRITE_FN(
    pandas_file_type: PandasFileType, value: pd.DataFrame
) -> PandasWriteFn:
    return cast(
        PandasWriteFn,
        {
            PandasFileType.CSV: value.to_csv,
            PandasFileType.JSON: value.to_json,
        }[pandas_file_type],
    )


def writeS3FileToDisk(
    value: SupportedS3DataType,
    tmp_filename: str,
    maybe_tagged_s3_subtype: Optional[task.TaggedS3SubType],
    converter_args: Dict[str, Any],
) -> None:
    if maybe_tagged_s3_subtype is not None:
        tagged_s3_subtype = cast(AnyDataFrame, maybe_tagged_s3_subtype)
        tag = tagged_s3_subtype.tag
        s3_subtype = tagged_s3_subtype.value
        if tag == task.S3TypeDataFrame:
            s3_type_dataframe = cast(task.S3TypeDataFrame, s3_subtype)
            value = cast(AnyDataFrame, value)
            has_geometry = s3_type_dataframe.has_geometry
            driver = s3_type_dataframe.driver
            if has_geometry:
                value.to_file(tmp_filename, driver=driver, **converter_args)  # type: ignore
            else:
                pandas_file_type = toPandasFileType(driver)
                to_file_fn = FILETYPE_TO_PANDAS_WRITE_FN(pandas_file_type, value)
                to_file_fn(tmp_filename, **converter_args)
        else:
            raise Exception(f"Unhandled write for S3 sub-type: {tag} -> {s3_subtype}")
    elif isinstance(value, IOBase):
        value.write(tmp_filename)
    else:
        raise Exception(f"Unhandled write for S3 file: {value}")
    return None


# Temporary location: not fully supported
class ObservationFile(object):
    def __init__(
        self,
        type_name: str,
        value: float,
        observation_group_id: Optional[int],
    ):
        self.type_name = type_name
        self.value = value
        self.observation_group_id = observation_group_id
