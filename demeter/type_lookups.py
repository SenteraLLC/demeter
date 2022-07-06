from typing import Union, Tuple, Type, Mapping, Dict

from .db.base_types import Table, TableKey

from . import data, task, work

type_table_lookup : Mapping[Type[Table], str] = {
  data.UnitType   : "unit_type",
  data.LocalType  : "data_type",
  data.CropType   : "crop_type",
  data.CropStage  : "crop_stage",
  data.ReportType : "report_type",
  data.LocalGroup : "data_group",
  task.HTTPType   : "http_type",
  task.S3Type     : "s3_type",
  task.S3TypeDataFrame : "s3_type_dataframe",
  task.FunctionType  : "task_type",
}


data_table_lookup : Mapping[Type[Table], str] = {
  data.Geom          : "geom",
  data.Owner         : "owner",
  data.Grower        : "grower",
  data.Field         : "field",
  data.LocalValue    : "data_value",
  data.GeoSpatialKey : "geospatial_key",
  data.TemporalKey   : "temporal_key",
  task.S3Output     : "s3_output",
  task.S3Object     : "s3_object",
  task.Function   : "task",
  work.Execution : "execution",
}


def sumMappings(*ms : Mapping[Type[Table], str]) -> Mapping[Type[Table], str]:
  out : Dict[Type[Table], str] = {}
  for m in ms:
    out.update(m.items())
  return out

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)


key_table_lookup : Mapping[Type[Table], Tuple[str, Type[TableKey]]] = {
  data.Planting     : ("taskting", data.PlantingKey),
  data.Harvest      : ("harvest",  data.HarvestKey),
  data.CropProgress : ("crop_progress", data.CropProgressKey),
  task.S3ObjectKey  : ("s3_object_key", task.S3ObjectKey),
  task.S3TypeDataFrame : ("s3_type_dataframe", task.S3TypeDataFrame),
  task.LocalParameter : ("data_parameter", task.LocalParameter),
  task.HTTPParameter     : ("http_parameter", task.HTTPParameter),
  task.S3InputParameter  : ("s3_input_parameter", task.S3InputParameter),
  task.S3OutputParameter : ("s3_output_parameter", task.S3OutputParameter),
  task.KeywordParameter : ("keyword_parameter", task.KeywordParameter),
  work.ExecutionKey     : ("work_key", work.ExecutionKey),
  work.LocalArgument    : ("data_argument", work.LocalArgument),
  work.HTTPArgument     : ("http_argument", work.HTTPArgument),
  work.KeywordArgument  : ("keyword_argument", work.KeywordArgument),
  work.S3InputArgument  : ("s3_input_argument", work.S3InputArgument),
  work.S3OutputArgument : ("s3_output_argument", work.S3OutputArgument),
}

