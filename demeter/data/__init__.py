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
    Parcel,
    ParcelGroup,
    Field,
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
    insertParcel,
    insertOrGetParcel,
    getParcel,
    getMaybeParcelId,
    insertParcelGroup,
    insertOrGetParcelGroup,
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
    FieldGroup,
    getFieldGroupAncestors,
    getOrgFields,
    FieldGroupFields,
    getFields,
    searchFieldGroup,
    make_field_group,
)

from ._local.types import (
    ObservationValue,
    ObservationType,
    UnitType,
    Operation,
)

from ._local.generated import (
    insertOperation,
    insertOrGetOperation,
    getMaybeOperationId,
    insertObservationValue,
    insertOrGetObservationValue,
    getMaybeObservationValue,
    getMaybeObservationValueId,
    insertObservationType,
    insertOrGetObservationType,
    getMaybeObservationTypeId,
    getObservationType,
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
    "Parcel",
    "insertParcel",
    "insertOrGetParcel",
    "ParcelGroup",
    "insertParcelGroup",
    "insertOrGetParcelGroup",
    "getParcel",
    "getMaybeParcelId",
    "Field",
    "insertField",
    "insertOrGetField",
    "getField",
    "getMaybeFieldId",
    "FieldGroup",
    "make_field_group",
    "FieldGroupFields",
    "getFieldGroupAncestors",
    "getOrgFields",
    "getFields",
    "searchFieldGroup",
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
    # Observations
    "ObservationValue",
    "insertObservationValue",
    "insertOrGetObservationValue",
    "getMaybeObservationValue",
    "getMaybeObservationValueId",
    "ObservationType",
    "insertObservationType",
    "insertOrGetObservationType",
    "getMaybeObservationTypeId",
    "getObservationType",
    "UnitType",
    "insertUnitType",
    "insertOrGetUnitType",
    "getMaybeUnitTypeId",
    "Operation",
    "insertOperation",
    "insertOrGetOperation",
    "getMaybeOperationId",
)
