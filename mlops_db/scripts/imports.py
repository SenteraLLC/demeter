from typing import Optional, Protocol, Any, TypeVar
from typing import cast

from dataclasses import dataclass

from ..lib.stdlib.imports import Import


class Grower(Import):
  # Explicit migrations
  GrowerId : int
  FarmName : str

  # Details
  RefNumber : str
  Region : str
  FarmCity : Optional[str]
  #Country : str
  FarmZipCode : int
  CountryId : int
  RegionId  : int


class Region(Import):
  Name : str

class Country(Import):
  Name : str

class GrowerField(Import):
  GrowerFieldId : int
  GrowerId : int
  Longitude : float
  LocationShape : str
  Latitude : float
  Name : str


class MeasureUnit(Import):
  MeasureUnitId : int

  MeasureUnitTypeId : int

  # These appear to always be the same or similar
  Identifier : str
  Description : str
  Name : str


class IrrigationApplied(Import):
  IrrigationAppliedId : int

  AppliedOffMeasureUnitId : int
  AppliedOnMeasureUnitId : int

  TotalNumberIrrigation : int

  IrrigationType : str
  SurfaceWater : int
  TotalWaterAppliedOn : Optional[int]
  TotalWaterAppliedOff : Optional[int]





# TODO: Don't use this now, but it's pretty awesome actually
#       It can be used to standardize units
class MeasureUnitConversion(Import):
  Factor : float
  MeasureUnitIdTo : int
  MeasureUnitIdFrom : int

# Not necessary it doesn't seem
class FieldIrrigation(Import):
  FieldIrrigationId : int
  IsIrrigated : bool
  IrrigatedType : str
  GrowerFieldId : int
# Project might be:
#   Trial
# Visit might be:
#   Showing up

# TODO: Meaning of Pre vs Post ReviewQuality
# TODO: Are the flags worth tracking?
class Review(Import):
  ReviewId : int

  ReviewHarvestId : int
  GrowerFieldId : int
  IrrigationAppliedId : int

  # ReviewQuality fkeys
  ReviewQualityPreId : int
  ReviewQualityPostId : int

  # Flags
  InsecticideApplied : bool
  FungicideApplied : bool
  FertilizerApplied : bool

  # Timestamps
  CreationDate : str
  LastUpdate : str

  # Other
  Status : int


# TODO: Are yield columns independent?
# Tracks barley globally,
#   sometimes easier to get malt barley over agronomic yield data
#   most is going to be malt
#   We prefer Agronomic because components: quality / quantity of grain
#   Agronomic -> pure bushels per acre
class ReviewHarvest(Import):
  ReviewHarvestId : int

  # mutex
  MaltBarleyYield : int
  MaltBarleyYieldMeasureUnitId : int

  AgronomicYield  : int
  AgronomicYieldMeasureUnitId : int

  HarvestDate     : str


# TODO: Are the DicId fields useful?
#  Probably not
# TODO: Are each of these groups independent?
class ReviewQuality(Import):
  ReviewQualityId : int

  # Always defined
  Enable         : bool
  CreationDate   : str
  LastUpdate     : str
  CreationUserId : str

  # Details
  IsExternalData: Optional[bool]
  # This always exists with Plump && Thins
  #  This almost always exists with GerminationCapacity
  PostHarvestMethodData: Optional[str]
  PostHarvestMethodDataDicId: Optional[int]

  # Germination
  GerminationCapacityMethodOther   : Optional[str]
  GerminationCapacityMethod        : Optional[str]
  GerminationCapacity              : Optional[int]

  # Thin Sieve
  ThinsSieve       : Optional[str]
  ThinsSieveOther  : Optional[int]
  Thins            : Optional[float]

  # Plump
  Plump            : Optional[float]
  PlumpSieve       : Optional[str]

  # Toxin protein, specific toxin
  DON: Optional[float]
  DONMeasureUnitId: Optional[int]
  # Won't get accepted, malt barley yield -> 0

  Protein: Optional[float]

  TestWeight: Optional[int]
  TestWeightMeasureUnitId: Optional[int]

  Sprout: Optional[int]
  SproutMethod: Optional[str]
  SproutMethodOther: Optional[str]
  SproutSecondsMeasureUnitId : Optional[int]
  SproutMethodDicId : Optional[int]


#Import = Union[Grower, Region, Country, GrowerField, MeasureUnit, IrrigationApplied, MeasureUnitConversion, FieldIrrigation, Review, ReviewHarvest, ReviewQuality]

