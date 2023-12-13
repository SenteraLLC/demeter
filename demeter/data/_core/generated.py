"""
Constructs insert functions for core types using `SQLGenerator`

The following functions are generated based on the schema. Using `getGeom()` as an example:
`getGeom()` is of type `GetTable()` which is a function that takes a table ID (here `geom_id`) and returns the demeter
type object associated with that ID (here it would be a `Geom` type). The demeter objects are used as a way to map these
function generation functions to the right ID and table. More specific functions should be written using SQL.
"""

from ...db import SQLGenerator
from ...db._generic_types import (
    GetId,
    GetTable,
    ReturnId,
)
from .._core import lookups as _lookups
from .._core.types import (
    Act,
    CropType,
    Field,
    FieldTrial,
    Organization,
    Plot,
)
from .grouper import Grouper
from .st_types import GeoSpatialKey, TemporalKey

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
    # key_table_lookup=_lookups.key_table_lookup,
)

# core types
getMaybeOrganizationId: GetId[Organization] = g.getMaybeIdFunction(Organization)
getMaybeGrouperId: GetId[Grouper] = g.getMaybeIdFunction(Grouper)
getMaybeFieldId: GetId[Field] = g.getMaybeIdFunction(Field)
getMaybeFieldTrialId: GetId[FieldTrial] = g.getMaybeIdFunction(FieldTrial)
getMaybePlotId: GetId[Plot] = g.getMaybeIdFunction(Plot)
getMaybeCropTypeId: GetId[CropType] = g.getMaybeIdFunction(CropType)
getMaybeActId: GetId[Act] = g.getMaybeIdFunction(Act)

getOrganization: GetTable[Organization] = g.getTableFunction(Organization)
getGrouper: GetTable[Grouper] = g.getTableFunction(Grouper)
getField: GetTable[Field] = g.getTableFunction(Field)
getFieldTrial: GetTable[FieldTrial] = g.getTableFunction(FieldTrial)
getPlot: GetTable[Plot] = g.getTableFunction(Plot)
getCropType: GetTable[CropType] = g.getTableFunction(CropType)
getAct: GetTable[Act] = g.getTableFunction(Act)

insertOrganization: ReturnId[Organization] = g.getInsertReturnIdFunction(Organization)
insertGrouper: ReturnId[Grouper] = g.getInsertReturnIdFunction(Grouper)
insertField: ReturnId[Field] = g.getInsertReturnIdFunction(Field)
insertFieldTrial: ReturnId[FieldTrial] = g.getInsertReturnIdFunction(FieldTrial)
insertPlot: ReturnId[Plot] = g.getInsertReturnIdFunction(Plot)
insertCropType: ReturnId[CropType] = g.getInsertReturnIdFunction(CropType)
insertAct: ReturnId[Act] = g.getInsertReturnIdFunction(Act)


insertOrGetOrganization: ReturnId[Organization] = g.partialInsertOrGetId(
    getMaybeOrganizationId, insertOrganization
)
insertOrGetGrouper: ReturnId[Grouper] = g.partialInsertOrGetId(
    getMaybeGrouperId, insertGrouper
)
insertOrGetField: ReturnId[Field] = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetFieldTrial: ReturnId[FieldTrial] = g.partialInsertOrGetId(
    getMaybeFieldTrialId, insertFieldTrial
)
insertOrGetPlot: ReturnId[Plot] = g.partialInsertOrGetId(getMaybePlotId, insertPlot)
"""
A note on the date_end (and created) columns of Field and FieldTrial:
    1. `date_end=None` is NOT VALID because setting explicitly to `None` bypasses the default `datetime.max`
    2. When insertOrGetField() is called, date_end is assigned an infinity value because of the schema.sql default.
        I don't know how to reconcile the SQL default with the Field() class default if `None` is passed.
    So, DO NOT pass `None` to either the `date_end` or "Detailed" columns (i.e., `details`, `created`,
    `last_updated`).
"""

insertOrGetCropType: ReturnId[CropType] = g.partialInsertOrGetId(
    getMaybeCropTypeId, insertCropType
)
insertOrGetAct: ReturnId[Act] = g.partialInsertOrGetId(getMaybeActId, insertAct)


# spatiotemporal types
getMaybeGeoSpatialKeyId: GetId[GeoSpatialKey] = g.getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId: GetId[TemporalKey] = g.getMaybeIdFunction(TemporalKey)

## Redundant with `getMaybeGeom()` in `geom.py`
# getGeom: GetTable[Geom] = g.getTableFunction(Geom)

insertGeoSpatialKey: ReturnId[GeoSpatialKey] = g.getInsertReturnIdFunction(
    GeoSpatialKey
)
insertTemporalKey: ReturnId[TemporalKey] = g.getInsertReturnIdFunction(TemporalKey)

insertOrGetGeoSpatialKey = g.partialInsertOrGetId(
    getMaybeGeoSpatialKeyId, insertGeoSpatialKey
)
insertOrGetTemporalKey = g.partialInsertOrGetId(
    getMaybeTemporalKeyId, insertTemporalKey
)
