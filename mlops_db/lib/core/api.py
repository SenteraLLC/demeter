from typing import Callable, TypeVar, Optional, Any

from functools import partial

from ..util.api_protocols import GetId, GetTable, ReturnId
from ..util.generators import getMaybeIdFunction, getInsertReturnIdFunction, getTableFunction, insertOrGetType
from ..util.type_lookups import AnyIdTable, AnyIdTable, AnyKeyTable

from .types import Field, Grower, GeoSpatialKey, TemporalKey, Owner, Geom


getMaybeFieldId          : GetId[Field]      = getMaybeIdFunction(Field)
getMaybeOwnerId          : GetId[Owner]      = getMaybeIdFunction(Owner)
getMaybeGrowerId         : GetId[Grower]      = getMaybeIdFunction(Grower)
getMaybeGeoSpatialKeyId  : GetId[GeoSpatialKey] = getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId    : GetId[TemporalKey] = getMaybeIdFunction(TemporalKey)

getField      : GetTable[Field]    = getTableFunction(Field)
getOwner      : GetTable[Owner]    = getTableFunction(Owner)
getGeom       : GetTable[Geom]     = getTableFunction(Geom)

insertField          : ReturnId[Field]      = getInsertReturnIdFunction(Field)
insertOwner          : ReturnId[Owner]      = getInsertReturnIdFunction(Owner)
insertGrower         : ReturnId[Grower]     = getInsertReturnIdFunction(Grower)
insertGeoSpatialKey : ReturnId[GeoSpatialKey] = getInsertReturnIdFunction(GeoSpatialKey)
insertTemporalKey : ReturnId[TemporalKey] = getInsertReturnIdFunction(TemporalKey)

insertOrGetGeoSpatialKey = partial(insertOrGetType, getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = partial(insertOrGetType, getMaybeTemporalKeyId, insertTemporalKey)

