from typing import Any, Callable, Mapping, Protocol, TypeVar, Union

import geopandas as gpd  # type: ignore

from .._datasource import DataSourceBase, ObservationFile, S3File
from .._types import ExecutionOutputs

SupportedOutputFile = Union[S3File, ObservationFile]

RawFunctionOutputs = Mapping[str, SupportedOutputFile]

WrappableFunction = Callable[..., RawFunctionOutputs]

T = TypeVar("T")
# TODO: Waiting for mypy to support Callable[[Foo, ...], Bar]
OutputLoadFunction = Callable[[DataSourceBase], gpd.GeoDataFrame]


class WrappedTransformation(Protocol):
    def __call__(self, **kwargs: Any) -> ExecutionOutputs:
        ...
