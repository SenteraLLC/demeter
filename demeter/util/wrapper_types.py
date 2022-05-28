from typing import Mapping, Union, Callable, Any, TypeVar, Protocol

import geopandas as gpd # type: ignore

from ..datasource.s3_file import S3File, LocalFile
from ..datasource.datasource import DataSourceBase
from ..types.execution import ExecutionOutputs

SupportedOutputFile = Union[S3File, LocalFile]

RawFunctionOutputs = Mapping[str, SupportedOutputFile]

WrappableFunction = Callable[..., RawFunctionOutputs]

T = TypeVar('T')
# TODO: Wait for mypy Support for Callable[[Foo, ...], Bar]
OutputLoadFunction = Callable[[DataSourceBase], gpd.GeoDataFrame]

class AddGeoDataFrameWrapper(Protocol):
    def __call__(self, **kwargs: Any) -> ExecutionOutputs: ...


