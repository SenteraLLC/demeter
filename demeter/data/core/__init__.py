from ...db.generic_types import GetId, GetTable, ReturnId, TypeToFunction
from ...db.type_to_sql import getMaybeIdFunction, getTableFunction, getInsertReturnIdFunction, partialInsertOrGetId

from .types import Field, Grower, GeoSpatialKey, TemporalKey, Owner, Geom, Coordinates, Polygon, MultiPolygon, Point, Line, TableId

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

insertOrGetGeoSpatialKey = partialInsertOrGetId(getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = partialInsertOrGetId(getMaybeTemporalKeyId, insertTemporalKey)
insertOrGetOwner = partialInsertOrGetId(getMaybeOwnerId, insertOwner)
insertOrGetGrower = partialInsertOrGetId(getMaybeGrowerId, insertGrower)
insertOrGetField = partialInsertOrGetId(getMaybeFieldId, insertField)

from .geom import getMaybeDuplicateGeom as getMaybeDuplicateGeom, \
                  insertOrGetGeom as insertOrGetGeom

from . import types

