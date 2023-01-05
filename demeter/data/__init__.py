from ._core.types import (
    Field,
    CropType,
    # ReportType,
    Act,
)

from ._core.st_types import (
    GeoSpatialKey,
    TemporalKey,
    Key,
    KeyIds,
    Geom,
)


from ._core.generated import (
    getFieldGroup,
    getMaybeFieldGroupId,
    insertOrGetFieldGroup,
    insertOrGetField,
    getField,
    getMaybeFieldId,
    getCropType,
    insertOrGetCropType,
    getMaybeCropTypeId,
    getGeom,
    insertOrGetGeoSpatialKey,
    getMaybeGeoSpatialKeyId,
    insertOrGetTemporalKey,
    getMaybeTemporalKeyId,
    getAct,
    insertOrGetAct,
    getMaybeActId,
)

from ._core.field_group import (
    FieldGroup,
    getFieldGroupAncestors,
    getOrgFields,
    FieldGroupFields,
    searchFieldGroup,
    getFieldGroupFields,
)

from ._core.geom import (
    getMaybeGeomId,
    insertOrGetGeom,
)

from ._observation.types import (
    Observation,
    ObservationType,
    UnitType,
)

from ._observation.generated import (
    insertOrGetObservation,
    getObservation,
    getMaybeObservationId,
    getUnitType,
    insertOrGetObservationType,
    getMaybeObservationTypeId,
    getObservationType,
    insertOrGetUnitType,
    getMaybeUnitTypeId,
)


__all__ = (
    # FieldGroup
    "FieldGroup",
    "getFieldGroup",
    "getMaybeFieldGroupId",
    "insertOrGetFieldGroup",
    "getFieldGroupAncestors",
    "getOrgFields",
    "FieldGroupFields",
    "searchFieldGroup",
    "getFieldGroupFields",
    # Field
    "Field",
    "getField",
    "getMaybeFieldId",
    "insertOrGetField",
    # CropType
    "CropType",
    "getCropType",
    "getMaybeCropTypeId",
    "insertOrGetCropType",
    # Act
    "Act",
    "getAct",
    "getMaybeActId",
    "insertOrGetAct",
    # Core spatiotemporal
    "GeoSpatialKey",
    "TemporalKey",
    "Key",
    "KeyIds",
    "insertOrGetGeoSpatialKey",
    "getMaybeGeoSpatialKeyId",
    "insertOrGetTemporalKey",
    "getMaybeTemporalKeyId",
    # Geom
    "Geom",
    "getGeom",
    "getMaybeGeomId",
    "insertOrGetGeom",
    # Observation
    "Observation",
    "getObservation",
    "getMaybeObservationId",
    "insertOrGetObservation",
    # ObservationType
    "ObservationType",
    "getObservationType",
    "getMaybeObservationTypeId",
    "insertOrGetObservationType",
    # UnitType
    "UnitType",
    "getUnitType",
    "getMaybeUnitTypeId",
    "insertOrGetUnitType",
)
