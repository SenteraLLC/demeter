from abc import ABC, abstractmethod

from typing import Dict, Any, TypedDict, List, Optional

import pandas as pd

from ..execution.types import ExecutionSummary, ExecutionArguments, ExecutionOutputs
from ..local.api import getMaybeLocalTypeId
from ..local.types import LocalType
from ..inputs.api import getS3TypeIdByName, getHTTPByName

from .types import DataSourceTypes, KeyToArgsFunction, ResponseFunction, OneToOneResponseFunction
from .s3_file import SupportedS3DataType


class DataSourceBase(ABC):
  def __init__(self,
               cursor : Any,
               function_id : int,
               execution_id : int,
              ):
    self.LOCAL = "__LOCAL"
    self.GEOM = "__PRIMARY_GEOMETRY"
    self.types = DataSourceTypes(
                   s3_type_ids    = {},
                   local_type_ids = {},
                   http_type_ids  = {},
                 )
    self.cursor = cursor

    self.execution_summary = ExecutionSummary(
                               function_id = function_id,
                               execution_id = execution_id,
                               inputs = ExecutionArguments(
                                          local = [],
                                          keyword = [],
                                          http = [],
                                          s3 = [],
                                          keys = [],
                                        ),
                               outputs = ExecutionOutputs(
                                           s3 = [],
                                         ),
                            )

  def _track_s3(self,
                type_name   : str,
               ) -> None:
    s3_type_id = getS3TypeIdByName(self.cursor, type_name)
    self.types["s3_type_ids"][s3_type_id] = type_name

  @abstractmethod
  def _s3(self,
         type_name   : str,
        ) -> SupportedS3DataType:
    raise NotImplemented

  def s3(self,
         type_name   : str,
        ) -> SupportedS3DataType:
    self._track_s3(type_name)
    return self._s3(type_name)


  def _track_local(self,
                   local_types : List[LocalType],
                  ) -> None:
    for t in local_types:
      maybe_local_type_id = getMaybeLocalTypeId(self.cursor, t)
      if maybe_local_type_id is None:
        raise Exception(f"Local Type does not exist: {t}")
      else:
        local_type_id = maybe_local_type_id
        self.types["local_type_ids"][local_type_id] = t

  @abstractmethod
  def _local(self, local_types : List[LocalType]) -> pd.DataFrame:
    raise NotImplemented

  def local(self, local_types : List[LocalType]) -> pd.DataFrame:
    self._track_local(local_types)
    return self._local(local_types)


  def _track_http(self,
                  type_name : str,
                 ) -> None:
    http_type_id, http_type = getHTTPByName(self.cursor, type_name)
    self.types["http_type_ids"][http_type_id] = type_name

  @abstractmethod
  def _http(self,
            type_name    : str,
            param_fn     : Optional[KeyToArgsFunction] = None,
            json_fn      : Optional[KeyToArgsFunction] = None,
            response_fn  : ResponseFunction = OneToOneResponseFunction,
            http_options : Dict[str, Any] = {}
           ) -> pd.DataFrame:
    raise NotImplemented

  def http(self,
           type_name    : str,
           param_fn     : Optional[KeyToArgsFunction] = None,
           json_fn      : Optional[KeyToArgsFunction] = None,
           response_fn  : ResponseFunction = OneToOneResponseFunction,
           http_options : Dict[str, Any] = {}
          ) -> pd.DataFrame:
    self._track_http(type_name)
    return self._http(type_name, param_fn, json_fn, response_fn, http_options)


