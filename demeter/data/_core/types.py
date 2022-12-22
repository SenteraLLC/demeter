from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ... import db


@dataclass(frozen=True)
class Field(db.Detailed):
    geom_id: db.TableId
    name: str
    field_group_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


@dataclass(frozen=True)
class CropType(db.TypeTable, db.Detailed):
    species: str
    cultivar: Optional[str] = None
    parent_id_1: Optional[db.TableId] = None
    parent_id_2: Optional[db.TableId] = None


@dataclass(frozen=True)
class CropStage(db.TypeTable):
    crop_stage: str


@dataclass(frozen=True)
class PlantingKey(db.TableKey):
    crop_type_id: db.TableId
    field_id: db.TableId
    planted: datetime


@dataclass(frozen=True)
class Planting(PlantingKey, db.Detailed):
    local_type_id: Optional[db.TableId] = None


@dataclass(frozen=True)
class Harvest(PlantingKey, db.Detailed):
    local_type_id: Optional[db.TableId] = None


@dataclass(frozen=True)
class CropProgressKey(PlantingKey):
    crop_stage_id: db.TableId


@dataclass(frozen=True)
class CropProgress(CropProgressKey, db.Detailed):
    pass


@dataclass(frozen=True)
class ReportType(db.TypeTable):
    report: str
