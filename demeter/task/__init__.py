from typing import Union

from . import function
from . import inputs

AnyTypeTable = Union[inputs.types.AnyTypeTable, function.types.AnyTypeTable]

AnyDataTable = Union[inputs.types.AnyDataTable, function.types.AnyDataTable]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, inputs.types.S3SubType]

AnyKeyTable = Union[inputs.types.AnyKeyTable,
                    function.types.AnyKeyTable,
                   ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]


from .function import *
from .function.types import *

from .inputs import *
from .inputs.types import *

__all__ = [
  'function',
  'inputs',

  # function
  'FunctionType',
  'Function',
  'FunctionId',
  'Parameter',
  'LocalParameter',
  'HTTPParameter',
  'S3InputParameter',
  'S3OutputParameter',
  'KeywordParameter',
  'S3TypeSignature',
  'FunctionSignature',

  'getMaybeFunctionTypeId',
  'insertFunctionWithMinor',
  'insertFunctionType',
  'insertLocalParameter',
  'insertHTTPParameter',
  'insertS3InputParameter',
  'insertS3OutputParameter',
  'insertKeywordParameter',
  'insertOrGetFunctionType',
  'insertFunction',
  'getLatestFunctionSignature',

  # inputs
  'HTTPVerb',
  'HTTPType',
  'KeywordType',
  'Keyword',
  'S3Type',
  'S3SubType',
  'S3TypeDataFrame',
  'S3Output',
  'S3Object',
  'S3ObjectKey',
  'TaggedS3SubType',

  'getMaybeHTTPTypeId',
  'getMaybeS3TypeId',
  'getHTTPType',
  'getS3Object',
  'getS3TypeBase',
  'getMaybeS3TypeDataFrame',
  'insertS3Object',
  'insertHTTPType',
  'insertS3TypeBase',
  'insertS3ObjectKey',
  'insertS3TypeDataFrame',
  'insertOrGetS3Type',
  'insertOrGetHTTPType',
  'insertOrGetS3TypeDataFrame',
  'insertS3Type',
  'getS3Type',
  'getS3TypeIdByName',
  'getHTTPByName',
  'insertS3ObjectKeys',
  'getS3ObjectByKey',
  'getS3ObjectByKeys',

]

