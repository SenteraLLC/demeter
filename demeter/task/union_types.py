from typing import Union

from . import inputs
from . import function

AnyTypeTable = Union[inputs.AnyTypeTable, function.AnyTypeTable]

AnyDataTable = Union[inputs.AnyDataTable, function.AnyDataTable]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, inputs.S3SubType]

AnyKeyTable = Union[inputs.AnyKeyTable,
                    function.AnyKeyTable,
                   ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]

