from .lib import schema_api as db
from .lib import types

import psycopg2
import psycopg2.extras
import psycopg2.extensions

import csv
import argparse
import os
import json
from datetime import datetime, date

from typing import List, Optional, Dict, Any, Set, TypedDict, Callable, TextIO, Tuple, Iterator

NO_DETAILS : types.Details = {}
NOW = datetime.now()

# TODO: What is date harvest_tuber vs harvest_vine
# TODO: Test the duplicate GEO detection function
# TODO: How to handle weather derived?

def pickFilenames(path : str, filenames : List[str], name : str) -> List[str]:
  picked : List[str] = []
  for f in filenames:
    if f.startswith(name):
      picked.append(os.path.join(path, f))
  return picked


def getProperty(feature : Dict[str, Any], name : str) -> str:
  return feature["properties"][name]


def maybeGetProperty(feature : Dict[str, Any], name : str) -> Optional[str]:
  try:
    return feature["properties"][name]
  except KeyError:
    pass
  return None


def loadGeometry(crs               : types.CRS,
                 feature           : Dict[str, Any],
                 container_geom_id : int = None,
                ) -> types.Geom:
  geometry = feature["geometry"]
  g = types.Geometry(
        type        = geometry["type"],
        coordinates = geometry["coordinates"],
        crs         = crs,
      )
  # TODO: How to deal with these projections? Not technically correct?
  if crs["properties"]["name"] == "urn:ogc:def:crs:OGC:1.3:CRS84":
    crs["properties"]["name"] = "urn:ogc:def:crs:EPSG::4326"
  return types.Geom(
            container_geom_id = container_geom_id,
            geom              = g,
            last_updated      = datetime.now(),
         )


def loadField(cursor  : Any,
              feature : Dict[str, Any],
              crs     : types.CRS,
             ) -> int:
  f = feature
  owner = types.Owner(
            owner = getProperty(f, "owner"),
          )
  owner_id = db.getMaybeOwnerId(cursor, owner)
  if owner_id is None:
    owner_id = db.insertOwner(cursor, owner)

  grower = types.Grower(
             farm  = getProperty(f, "farm"),
           )
  grower_id = db.getMaybeGrowerId(cursor, grower)
  if grower_id is None:
    grower_id = db.insertGrower(cursor, grower)

  geom = loadGeometry(crs, f)
  geom_id = db.insertGeom(cursor, geom)

  year = getProperty(f, "year")
  field = types.Field(
            owner_id = owner_id,
            year = int(year),
            geom_id = geom_id,
            grower_id = grower_id,
          )
  maybe_field_id = db.getMaybeFieldId(cursor, field)
  if maybe_field_id is not None:
    return maybe_field_id
  return db.insertField(cursor, field)


def loadFieldFile(cursor, filename : str) -> Dict[str, int]:
  with open(filename) as geojson_file:
    contents        = json.load(geojson_file)
    crs : types.CRS = contents["crs"]
    field_id_map = {}
    for f in contents["features"]:
      field_tag = getProperty(f, "field_id")
      field_id = loadField(cursor, f, crs)
      field_id_map[field_tag] = field_id
    return field_id_map


# Dates in the datafiles are a mess
def parseDate(d : str) -> datetime:
  delims = "tT "
  for delim in delims:
    try:
      maybe_delim = d.index(delim)
      d = d[:maybe_delim]
      break
    except ValueError:
      pass

  try:
    return datetime.strptime(d, "%m/%d/%Y")
  except ValueError:
    pass

  try:
    return datetime.strptime(d, "%Y-%m-%d")
  except ValueError:
    pass

  return datetime.strptime(d, "%Y/%m/%d")


def maybeParseDate(d : Optional[str]) -> Optional[datetime]:
  if d is not None and d.lower() != "na":
    return parseDate(d)
  return None


class ParseMeta(TypedDict):
  cursor       : Any
  filename     : str
  field_id_map : Dict[str, int]


RowAndMaybeGeomId = Tuple[Dict[str, str], Dict[str, Optional[str]], Optional[int]]

def maybeLower(s : Any) -> Any:
  if isinstance(s, str):
    return s.lower()
  return s

def yieldCSVRow(file                : TextIO,
                cursor              : Any,
                required_properties : Set[str],
                optional_properties : Set[str],
               ) -> Iterator[RowAndMaybeGeomId]:
  reader = csv.DictReader(file)
  for row in reader:

    required : Dict[str, str] = {}
    required["field_tag"] = row["field_id"]
    for p in required_properties:
      required[p] = maybeLower(row[p])

    optional : Dict[str, Optional[str]] = {}
    for p in optional_properties:
      v = row.get(p)
      if v is not None:
        v = maybeLower(v)
      optional[p] = v

    yield required, optional, None


# TODO: Type for GeoJson file
def yieldGeoJsonRow(file : TextIO,
                    cursor              : Any,
                    required_properties : Set[str],
                    optional_properties : Set[str],
                   ) -> Iterator[RowAndMaybeGeomId]:
  contents = json.load(file)
  features = contents["features"]

  for f in features:
    geom_id : Optional[int] = None
    if maybeGetProperty(f, "geometry") is not None:
      crs = contents["crs"]
      geom   = loadGeometry(crs, f)
      geom_id = db.insertGeom(cursor, geom)

    required : Dict[str, str] = {"field_tag" : getProperty(f, "field_id")}
    for p in required_properties:
      required[p] = maybeLower(getProperty(f, p))

    optional : Dict[str, Optional[str]] = {}
    for p in optional_properties:
       v = getProperty(f, p)
       if v is not None:
         v = maybeLower(v)
       optional[p] = maybeGetProperty(f, p)

    yield required, optional, geom_id


YieldFn = Callable[[TextIO, Any, Set[str], Set[str]], Iterator[RowAndMaybeGeomId]]

def loadPlantingFile(parse_meta : ParseMeta,
                     yield_fn   : YieldFn,
                    ) -> List[types.PlantingKey]:
  cursor       = parse_meta["cursor"]
  filename     = parse_meta["filename"]
  field_id_map = parse_meta["field_id_map"]

  required_properties = set(["crop"])
  optional_properties = set(["variety", "date_plant", "date_emerge"])

  planting_keys : List[types.PlantingKey] = []
  with open(filename) as f:
    for required, optional, geom_id in yield_fn(f, cursor, required_properties, optional_properties):
      field_tag = required["field_tag"]
      field_id  = field_id_map[field_tag]
      field = db.getField(cursor, field_id)

      if geom_id is None:
        geom_id = field["geom_id"]

      crop_type = types.CropType(species  = required["crop"],
                                 cultivar = optional["variety"],
                                )
      crop_type_id = db.insertOrGetCropType(cursor, crop_type)

      date_planted = maybeParseDate(optional["date_plant"])

      planting = types.Planting(
                   field_id     = field_id,
                   crop_type_id = crop_type_id,
                   geom_id      = geom_id,
                   completed    = date_planted,
                   last_updated = NOW,
                   details      = NO_DETAILS,
                 )
      planting_key = db.insertPlanting(cursor, planting)
      planting_keys.append(planting_key)

      crop_stage = types.CropStage(crop_stage = "emergence")
      crop_stage_id = db.insertOrGetCropStage(cursor, crop_stage)

      date_emerged = maybeParseDate(optional["date_emerge"])
      planting_geom_id = planting_key["geom_id"]
      crop_progress = types.CropProgress(
                        field_id = field_id,
                        crop_type_id = crop_type_id,
                        planting_geom_id = planting_geom_id,
                        geom_id = geom_id,
                        crop_stage_id = crop_stage_id,
                        day = date_emerged,
                      )
      crop_progress_key = db.insertCropProgress(cursor, crop_progress)

  return planting_keys


def insertLocalValue(cursor         : Any,
                     quantity       : float,
                     unit_type_id   : int,
                     geom_id        : int,
                     field_id       : int,
                     local_group_id : Optional[int],
                     acquired       : datetime,
                    ) -> int:

  # TODO: Test details
  # TODO: Test loading a geometry back into python
  local_value = types.LocalValue(
                  geom_id        = geom_id,
                  field_id       = field_id,
                  quantity       = quantity,
                  last_updated   = NOW,
                  unit_type_id   = unit_type_id,
                  local_group_id = local_group_id,
                  acquired       = acquired,
                  details        = NO_DETAILS,
                )
  local_value_id = db.insertLocalValue(cursor, local_value)

  return local_value_id



def loadAppliedFile(parse_meta : ParseMeta,
                    yield_fn   : YieldFn,
                   ) -> Set[int]:
  cursor       = parse_meta["cursor"]
  filename     = parse_meta["filename"]
  field_id_map = parse_meta["field_id_map"]

  required_properties = set(["source_n", "date_applied", "rate_n_kgha"])
  optional_properties : Set[str] = set()

  with open(filename) as f:
    local_ids = set()
    for required, optional, geom_id in yield_fn(f, cursor, required_properties, optional_properties):
      field_tag = required["field_tag"]
      field_id  = field_id_map[field_tag]
      field = db.getField(cursor, field_id)

      if geom_id is None:
        geom_id = field["geom_id"]

      source_keys : Dict[str, str] = {
        "n"   : "nitrogen",
        "uan" : "urea ammonium nitrate"
      }
      applied = source_keys[required["source_n"]]

      local_type = types.LocalType(type_name=applied, type_category="application")
      local_type_id = db.insertOrGetLocalType(cursor, local_type)

      TYPE_KEY = "rate_n_kgha"
      supported_units = {"rate_n_kgha": "kg/ha"}
      unit = supported_units[TYPE_KEY]

      unit_type = types.UnitType(unit=unit, local_type_id=local_type_id)
      unit_type_id = db.insertOrGetUnitType(cursor, unit_type)

      acquired = parseDate(required["date_applied"])
      quantity = float(required[TYPE_KEY])

      local_id = insertLocalValue(cursor, quantity, unit_type_id, geom_id, field_id, None, acquired)

      local_ids.add(local_id)

    return local_ids


def loadSentinelFile(parse_meta : ParseMeta,
                     _          : YieldFn,
                    ) -> Dict[int, Set[int]]:
  cursor       = parse_meta["cursor"]
  filename     = parse_meta["filename"]
  field_id_map = parse_meta["field_id_map"]

  local_groups : Dict[int, Set[int]] = {}
  with open(filename) as geojson_file:
    contents        = json.load(geojson_file)
    crs : types.CRS = contents["crs"]
    features = contents["features"]
    for f in features:
      field_tag = getProperty(f, "field_id")
      field_id = field_id_map[field_tag]
      field = db.getField(cursor, field_id)

      geom = loadGeometry(crs, f)
      geom_id = db.insertGeom(cursor, geom)

      date = getProperty(f, "acquisition_time")
      acquired = parseDate(date)

      simplify_sensor_type = lambda s : s.split()[0].lower()
      sensor_name = simplify_sensor_type(getProperty(f, "sensor_type"))
      local_group = types.LocalGroup(
                      group_name     = sensor_name,
                      group_category = "sentinel"
                    )
      local_group_id = db.insertOrGetLocalGroup(cursor, local_group)

      prefix = "wl_"

      wavelengths = {k[len(prefix):] : float(v) for k, v in f["properties"].items() if k.startswith(prefix)}

      local_ids : Set[int] = set()
      type_category = "wavelength"
      for wavelength, quantity in wavelengths.items():
        local_type = types.LocalType(type_name=wavelength, type_category=type_category)
        local_type_id = db.insertOrGetLocalType(cursor, local_type)

        unit = "reflectance"
        unit_type_id = db.insertOrGetUnitType(cursor, types.UnitType(unit=unit, local_type_id=local_type_id))

        local_id = insertLocalValue(cursor, quantity, unit_type_id, geom_id, field_id, local_group_id, acquired)

        local_ids.add(local_id)

      if local_group_id is not None:
        local_groups[local_group_id] = local_ids
  return local_groups


def loadWeatherFile(parse_meta : ParseMeta,
                    _          : YieldFn,
                   ) -> Dict[int, Set[int]]:
  cursor       = parse_meta["cursor"]
  filename     = parse_meta["filename"]
  field_id_map = parse_meta["field_id_map"]

  local_groups : Dict[int, Set[int]] = {}
  with open(filename) as geojson_file:
    print(filename)
    contents        = json.load(geojson_file)
    crs : types.CRS = contents["crs"]
    features = contents["features"]
    for f in features:
      geom = f["geometry"]
      some_point = types.Geometry(
                      type = geom["type"],
                      coordinates = geom["coordinates"],
                      crs = crs
                   )
      stmt = """select geom_id from geom where ST_Contains(geom.geom, %(geom)s)"""
      cursor.execute(stmt, {"geom": json.dumps(some_point)})
      results = cursor.fetchall()
      if len(results):
        print("Contained by: ",len(results))
        # TODO: Huh... points arent in any geom boundaries
        #       Seems like this data isn't even used in geoml
  return local_groups


def loadWeatherDerivedFile(parse_meta : ParseMeta,
                           yield_fn   : YieldFn,
                          ) -> Dict[int, Set[int]]:
  cursor       = parse_meta["cursor"]
  filename     = parse_meta["filename"]
  field_id_map = parse_meta["field_id_map"]

  local_groups : Dict[int, Set[int]] = {}
  with open(filename) as csv_file:
    reader = csv.DictReader(csv_file)

    blacklist = ["owner", "farm", "field_id", "year", "date"]
    data_fields : List[str] = []
    if reader.fieldnames is not None:
      data_fields = [f for f in reader.fieldnames if f not in blacklist]

    getUnitAbbreviation = lambda f : f.split("_")[1]

    planting_keys : List[types.PlantingKey] = []
    for row in reader:
      field_tag = row["field_id"]
      field_id = field_id_map[field_tag]
      field = db.getField(cursor, field_id)
      geom_id = field["geom_id"]

      date = row["date"]
      acquired = parseDate(date)

      abbr_to_unit = {"c"    : "celcius",
                      "f"    : "fahrenheit",
                      "in"   : "inches",
                      "mm"   : "millimeters",
                      "mjm2" : "mj/m2",
                     }

      local_group = types.LocalGroup(
                      group_name="weather_derived",
                      group_category=None,
                    )
      local_group_id = db.insertOrGetLocalGroup(cursor, local_group)

      local_ids : Set[int] = set()
      for field_name in data_fields:
        quantity = float(row[field_name])

        local_type = types.LocalType(type_name=field_name, type_category=None)
        local_type_id = db.insertOrGetLocalType(cursor, local_type)

        unit = abbr_to_unit[getUnitAbbreviation(field_name)]
        unit_type_id = db.insertOrGetUnitType(cursor, types.UnitType(unit=unit, local_type_id=local_type_id))

        local_id = insertLocalValue(cursor, quantity, unit_type_id, geom_id, field_id, local_group_id, acquired)

        local_ids.add(local_id)

      local_groups[local_group_id] = local_ids

  return local_groups


OBS_PREFIX = "obs_"

def loadSampleFile(parse_meta : ParseMeta,
                   yield_fn   : YieldFn,
                  ) -> Set[int]:
  cursor       = parse_meta["cursor"]
  filename     = parse_meta["filename"]
  field_id_map = parse_meta["field_id_map"]

  suffix = os.path.splitext(filename)[1]

  basename = os.path.basename(filename)
  key = basename[len(OBS_PREFIX):-len(suffix)]
  # Handle suffixed obs files
  key_parts = key.split("_")
  if len(key_parts) > 1:
    key = key_parts[0]

  required_properties = set([key, "date", "measure", "value"])
  optional_properties : Set[str] = set()

  with open(filename) as f:
    local_ids = set()
    for required, optional, geom_id in yield_fn(f, cursor, required_properties, optional_properties):
      field_tag = required["field_tag"]
      field_id  = field_id_map[field_tag]
      field = db.getField(cursor, field_id)

      if geom_id is None:
        geom_id = field["geom_id"]

      if key is not None:
        local_type = required[key]
        local_type_id = db.insertOrGetLocalType(cursor, types.LocalType(type_name=local_type, type_category=key))

        date = required["date"]
        acquired = parseDate(date)

        unit = required["measure"]
        unit_type_id = db.insertOrGetUnitType(cursor, types.UnitType(unit=unit, local_type_id=local_type_id))

        quantity = float(required["value"])

        # TODO: Make sure this geom intersects with provided?

        local_id = insertLocalValue(cursor, quantity, unit_type_id, geom_id, field_id, None, acquired)

        local_ids.add(local_id)

    return local_ids




def loadFieldIdMap(filename : str) -> Dict[str, int]:
  if not os.path.exists(filename):
    return {}

  with open(filename) as f:
    return json.load(f)


def loadDataFolder(hostname : str, data_path : str, field_id_map : Dict[str, int]):
  psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options, port=8765)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  print("Loading Data Directory: ",data_path)

  # TODO: Do something with return values?
  to_parse : Dict[str, Callable[[ParseMeta, YieldFn], Any]] = {
    "as_planted"     : loadPlantingFile,
    "n_applications" : loadAppliedFile,
    "rs_sentinel"    : loadSentinelFile,
     OBS_PREFIX      : loadSampleFile,
    #"weather\."       : loadWeatherFile,  # TODO: How/if to assign this to a field?
    "weather_derived" : loadWeatherDerivedFile,
  }

  data_files = os.listdir(data_path)
  field_bounds_files = pickFilenames(data_path, data_files, "field_bounds")
  for bounds_file in field_bounds_files:
    field_id_map.update(loadFieldFile(cursor, bounds_file))

  for prefix, loadFunction in to_parse.items():
    files = pickFilenames(data_path, data_files, prefix)
    for filename in files:
      print("  ",os.path.basename(filename))

      suffix = os.path.splitext(filename)[1][1:]
      yield_fn = {"csv"     : yieldCSVRow,
                  "geojson" : yieldGeoJsonRow,
                 }.get(suffix)
      if yield_fn is None:
        raise Exception("Unknown local format")

      parse_meta = ParseMeta(
                     cursor       = cursor,
                     filename     = filename,
                     field_id_map = field_id_map,
                   )
      loadFunction(parse_meta, yield_fn)

  connection.commit()


# TODO: Just hard-coding this information until it can be better organized
def loadData(hostname  : str,
             data_path : str,
             field_id_map : Dict[str, int],
            ) -> None:
  data_folders = sorted([f for f in os.listdir(data_path) if f.endswith("css")])
  for f in data_folders:
    path = os.path.join(data_path, f)
    if f == "2021_css":
      sub_2021_initial_path = os.path.join(path, "initial")
      loadDataFolder(hostname, sub_2021_initial_path, field_id_map)
      update_path = os.path.join(path, "update")
      for update_dir in os.listdir(update_path):
        update_dir_path = os.path.join(update_path, update_dir)
        loadDataFolder(hostname, update_dir_path, field_id_map)
    else:
      loadDataFolder(hostname, path, field_id_map)
      pass


def main(hostname : str, data_path : str, field_id_path : str):
  # TODO: This field_map is kind of a hack but some files reference fields from other directories
  field_id_map = loadFieldIdMap(field_id_path)
  loadData(hostname, data_path, field_id_map)
  with open(field_id_path, "w") as f:
    json.dump(field_id_map, f)



if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Process some integers.')
  parser.add_argument('--host', type=str, help='postgres hostname', required=True)
  parser.add_argument('--data_path', type=str, help='path to folder containing mlops data', required=True)
  parser.add_argument('--field_id_path', type=str, help='Path to field id path file, required to populate some data', required=True)
  args = parser.parse_args()
  main(args.host, args.data_path, args.field_id_path)

