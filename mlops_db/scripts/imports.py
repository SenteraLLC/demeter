from typing import Optional, Protocol, Any, TypeVar
from typing import cast

from dataclasses import dataclass

# TODO: Need to replace this with a proper library
#       This is a super sneaky hack to make dicts look like protocols
class Import(Protocol):
  def __getattr__(self, attr : str): ...

class DictWrapper(dict):
  def __getattr__(self, attr : str):
    return self.get(attr)

I = TypeVar('I', bound=Import)

def WrapImport(i : I) -> I:
  return cast(I, DictWrapper(cast(Any, i)))


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

  GerminationCapacityMethodOther   : Optional[str]
  GerminationCapaticityMethodDicId : Optional[int]
  GerminationCapacityMethod        : Optional[str]
  GerminationCapacity              : Optional[int]

  ThinsSieveDictId : Optional[int]
  ThinsSieve       : Optional[str]
  ThinsSieveOther  : Optional[int]
  Thins            : Optional[float]

  Plump            : Optional[float]
  PlumpSieve       : Optional[str]
  PlumpSieveDicId  : Optional[int]

  # Toxin protein, specific toxin
  DON: Optional[float]
  DONMeasureUnitId: Optional[int]
  # Won't get accepted, malt barley yield -> 0

  Protein: Optional[float]

  TestWeight: Optional[int]
  TestWeightMeasureUnitId: Optional[int]
  IsExternalData: Optional[bool]

  Sprout: Optional[int]
  SproutMethod: Optional[str]
  SproutMethodOther: Optional[str]
  SproutMethodDicId : Optional[int]

  # This always exists with Plump && Thins
  #  This almost always exists with GerminationCapacity
  PostHarvestMethodData: Optional[str]
  PostHarvestMethodDataDicId: Optional[int]


#Import = Union[Grower, Region, Country, GrowerField, MeasureUnit, IrrigationApplied, MeasureUnitConversion, FieldIrrigation, Review, ReviewHarvest, ReviewQuality]

