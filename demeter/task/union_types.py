from typing import Union

from . import inputs
from . import function

AnyTypeTable = Union[inputs.types.AnyTypeTable, function.types.AnyTypeTable]

AnyDataTable = Union[inputs.types.AnyDataTable, function.types.AnyDataTable]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, inputs.types.S3SubType]

AnyKeyTable = Union[inputs.types.AnyKeyTable,
                    function.types.AnyKeyTable,
                   ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]

