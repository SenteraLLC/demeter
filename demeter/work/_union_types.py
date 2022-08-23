from typing import Union

from ._types import *

from ..db import TypeTable

AnyKeyTable = Union[LocalArgument, HTTPArgument, S3InputArgument, S3OutputArgument, KeywordArgument, ExecutionKey]

class _Ignore(TypeTable):
  pass
AnyTypeTable = Union[_Ignore]

AnyDataTable = Union[Execution]

AnyIdTable = Union[AnyTypeTable, AnyDataTable]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]

