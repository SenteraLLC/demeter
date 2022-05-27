from typing import Any, Optional, Callable, List, Dict

import pandas as pd
import geopandas as gpd # type: ignore

from ..types.inputs import LocalType

from .base import DataSourceBase
from .s3_file import SupportedS3DataType
from .types import KeyToArgsFunction, ResponseFunction, OneToOneResponseFunction


class DataSourceRegister(DataSourceBase):
  def __init__(self,
               cursor : Any,
               function_id : int = 0,
               execution_id : int = 0,
              ):
    super().__init__(cursor, function_id, execution_id)


  def _s3(self,
          type_name   : str,
         ) -> SupportedS3DataType:
    return pd.DataFrame()


  def _local(self, local_types : List[LocalType]) -> pd.DataFrame:
    return pd.DataFrame()


  def _http(self,
           type_name    : str,
           param_fn     : Optional[KeyToArgsFunction] = None,
           json_fn      : Optional[KeyToArgsFunction] = None,
           response_fn  : ResponseFunction = OneToOneResponseFunction,
           http_options : Dict[str, Any] = {}
          ) -> pd.DataFrame:
    return pd.DataFrame()


  def getMatrix(self) -> gpd.GeoDataFrame:
    return pd.DataFrame()


  def join(self,
           left_type_name : str,
           right_type_name : str,
           join_fn : Optional[Callable[..., Any]] = None,
           **kwargs : Any,
          ) -> None:
    return None

