from typing import Mapping, Union, Callable, Any, TypeVar, Protocol

import geopandas as gpd # type: ignore

from ..datasource import S3File, LocalFile
from ..datasource import DataSourceBase
from ..types import ExecutionOutputs

SupportedOutputFile = Union[S3File, LocalFile]

RawFunctionOutputs = Mapping[str, SupportedOutputFile]

WrappableFunction = Callable[..., RawFunctionOutputs]

T = TypeVar('T')
# TODO: Waiting for mypy to support Callable[[Foo, ...], Bar]
OutputLoadFunction = Callable[[DataSourceBase], gpd.GeoDataFrame]

class WrappedTransformation(Protocol):
    def __call__(self, **kwargs: Any) -> ExecutionOutputs: ...

