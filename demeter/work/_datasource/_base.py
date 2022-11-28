from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict

import pandas as pd

from ... import data, db, task
from .._types import (
    ExecutionArguments,
    ExecutionKey,
    ExecutionOutputs,
    ExecutionSummary,
)
from ._response import KeyToArgsFunction, OneToOneResponseFunction, ResponseFunction
from ._s3_file import SupportedS3DataType


class DataSourceTypes(TypedDict):
    s3_type_ids: Dict[db.TableId, str]
    local_type_ids: Dict[db.TableId, data.LocalType]
    http_type_ids: Dict[db.TableId, str]


class DataSourceBase(ABC):
    def __init__(
        self,
        cursor: Any,
        function_id: db.TableId,
        execution_id: db.TableId,
    ):
        self.OBSERVATION = "__OBSERVATION"
        self.GEOM = "__PRIMARY_GEOMETRY"
        self.types = DataSourceTypes(
            s3_type_ids={},
            local_type_ids={},
            http_type_ids={},
        )
        self.cursor = cursor

        self.execution_summary = ExecutionSummary(
            function_id=function_id,
            execution_id=execution_id,
            inputs=ExecutionArguments(
                local=[],
                keyword=[],
                http=[],
                s3=[],
                keys=[],
            ),
            outputs=ExecutionOutputs(
                s3=[],
            ),
        )

    def _track_s3(
        self,
        type_name: str,
    ) -> None:
        s3_type_id = task.getS3TypeIdByName(self.cursor, type_name)
        self.types["s3_type_ids"][s3_type_id] = type_name

    @abstractmethod
    def _s3(
        self,
        type_name: str,
    ) -> SupportedS3DataType:
        raise NotImplemented

    def s3(
        self,
        type_name: str,
    ) -> SupportedS3DataType:
        self._track_s3(type_name)
        return self._s3(type_name)

    def _track_local(
        self,
        local_types: List[data.LocalType],
    ) -> None:
        for t in local_types:
            maybe_local_type_id = data.getMaybeLocalTypeId(self.cursor, t)
            if maybe_local_type_id is None:
                raise Exception(f"Local Type does not exist: {t}")
            else:
                local_type_id = maybe_local_type_id
                self.types["local_type_ids"][local_type_id] = t

    @abstractmethod
    def _local(self, local_types: List[data.LocalType]) -> pd.DataFrame:
        raise NotImplemented

    def local(self, local_types: List[data.LocalType]) -> pd.DataFrame:
        self._track_local(local_types)
        return self._local(local_types)

    def _track_http(
        self,
        type_name: str,
    ) -> None:
        http_type_id, http_type = task.getHTTPTypeByName(self.cursor, type_name)
        self.types["http_type_ids"][http_type_id] = type_name

    @abstractmethod
    def _http(
        self,
        type_name: str,
        param_fn: Optional[KeyToArgsFunction] = None,
        json_fn: Optional[KeyToArgsFunction] = None,
        response_fn: ResponseFunction = OneToOneResponseFunction,
        http_options: Dict[str, Any] = {},
    ) -> pd.DataFrame:
        raise NotImplemented

    def http(
        self,
        type_name: str,
        param_fn: Optional[KeyToArgsFunction] = None,
        json_fn: Optional[KeyToArgsFunction] = None,
        response_fn: ResponseFunction = OneToOneResponseFunction,
        http_options: Dict[str, Any] = {},
    ) -> pd.DataFrame:
        self._track_http(type_name)
        return self._http(type_name, param_fn, json_fn, response_fn, http_options)
