from typing import Any, Callable, Dict, List, Optional

import geopandas as gpd  # type: ignore
import pandas as pd

from ... import data, db
from ._base import DataSourceBase
from ._response import (KeyToArgsFunction, OneToOneResponseFunction,
                        ResponseFunction)
from ._s3_file import SupportedS3DataType


class DataSourceRegister(DataSourceBase):
    # TODO: Fix weird defaults, maybe use a sentinel value
    def __init__(
        self,
        cursor: Any,
        function_id: db.TableId = db.TableId(0),
        execution_id: db.TableId = db.TableId(0),
    ):
        super().__init__(cursor, function_id, execution_id)

    def _s3(
        self,
        type_name: str,
    ) -> SupportedS3DataType:
        return pd.DataFrame()

    def _local(self, local_types: List[data.LocalType]) -> pd.DataFrame:
        return pd.DataFrame()

    def _http(
        self,
        type_name: str,
        param_fn: Optional[KeyToArgsFunction] = None,
        json_fn: Optional[KeyToArgsFunction] = None,
        response_fn: ResponseFunction = OneToOneResponseFunction,
        http_options: Dict[str, Any] = {},
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def getMatrix(self) -> gpd.GeoDataFrame:
        return pd.DataFrame()

    def join(
        self,
        left_type_name: str,
        right_type_name: str,
        join_fn: Optional[Callable[..., Any]] = None,
        **kwargs: Any,
    ) -> None:
        return None
