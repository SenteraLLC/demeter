from ._core.types import (
    # TODO: Replace these with their library counterparts
    Point,
    Line,
    Polygon,
    MultiPolygon,
    Coordinates,
    CRS,
    GeomImpl,
    Geom,
    Field,
    FieldGroup,
    CropType,
    CropStage,
    CropProgress,
    CropProgressKey,
    Planting,
    PlantingKey,
    GeoSpatialKey,
    TemporalKey,
    Key,
    KeyIds,
    ReportType,
)


from ._core.generated import (
    getGeom,
    insertFieldGroup,
    insertOrGetFieldGroup,
    insertField,
    insertOrGetField,
    getField,
    getMaybeFieldId,
    getPlanting,
    insertPlanting,
    insertOrGetPlanting,
    insertCropType,
    insertOrGetCropType,
    getMaybeCropTypeId,
    insertCropStage,
    insertOrGetCropStage,
    getMaybeCropStageId,
    insertCropProgress,
    insertOrGetCropProgress,
    getCropProgress,
    insertGeoSpatialKey,
    insertOrGetGeoSpatialKey,
    getMaybeGeoSpatialKeyId,
    insertTemporalKey,
    insertOrGetTemporalKey,
    getMaybeTemporalKeyId,
    insertReportType,
    getMaybeReportTypeId,
)

from ._core.geom import (
    getMaybeGeomId,
    insertOrGetGeom,
)

from ._core.field_group import (
    getFieldGroupAncestors,
    getOrgFields,
    FieldGroupFields,
    searchFieldGroup,
    getFieldGroupFields,
)

from ._local.types import (
    LocalValue,
    LocalType,
    UnitType,
    Act,
)

from ._local.generated import (
    insertAct,
    insertOrGetAct,
    getMaybeActId,
    insertLocalValue,
    insertOrGetLocalValue,
    getMaybeLocalValue,
    getMaybeLocalValueId,
    insertLocalType,
    insertOrGetLocalType,
    getMaybeLocalTypeId,
    getLocalType,
    insertUnitType,
    insertOrGetUnitType,
    getMaybeUnitTypeId,
)

__all__ = (
    "Point",
    "Line",
    "Polygon",
    "MultiPolygon",
    "Coordinates",
    "CRS",
    "GeomImpl",
    "Geom",
    "getGeom",
    "getMaybeGeomId",
    "insertOrGetGeom",
    "FieldGroup",
    "insertFieldGroup",
    "insertOrGetFieldGroup",
    "Field",
    "insertField",
    "insertOrGetField",
    "getField",
    "getMaybeFieldId",
    "FieldGroup",
    "getFieldGroupAncestors",
    "getOrgFields",
    "FieldGroupFields",
    "searchFieldGroup",
    "getFieldGroupFields",
    "insertFieldGroup",
    "Planting",
    "PlantingKey",
    "getPlanting",
    "insertPlanting",
    "insertOrGetPlanting",
    "CropType",
    "insertCropType",
    "insertOrGetCropType",
    "getMaybeCropTypeId",
    "CropStage",
    "insertCropStage",
    "insertOrGetCropStage",
    "getMaybeCropStageId",
    "CropProgress",
    "CropProgressKey",
    "insertCropProgress",
    "insertOrGetCropProgress",
    "getCropProgress",
    "Key",
    "KeyIds",
    "GeoSpatialKey",
    "insertGeoSpatialKey",
    "insertOrGetGeoSpatialKey",
    "getMaybeGeoSpatialKeyId",
    "TemporalKey",
    "insertTemporalKey",
    "insertOrGetTemporalKey",
    "getMaybeTemporalKeyId",
    "ReportType",
    "insertReportType",
    "getMaybeReportTypeId",
    # Locals
    "LocalValue",
    "insertLocalValue",
    "insertOrGetLocalValue",
    "getMaybeLocalValue",
    "getMaybeLocalValueId",
    "LocalType",
    "insertLocalType",
    "insertOrGetLocalType",
    "getMaybeLocalTypeId",
    "getLocalType",
    "UnitType",
    "insertUnitType",
    "insertOrGetUnitType",
    "getMaybeUnitTypeId",
    "Act",
    "insertAct",
    "insertOrGetAct",
    "getMaybeActId",
)
