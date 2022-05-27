from typing import Union, Dict

from ..types import local, inputs, function, execution, core

type_table_lookup = {
  local.UnitType   : "unit_type",
  local.LocalType  : "local_type",
  local.CropType   : "crop_type",
  local.CropStage  : "crop_stage",
  local.ReportType : "report_type",
  local.LocalGroup : "local_group",
  inputs.HTTPType   : "http_type",
  inputs.S3Type     : "s3_type",
  inputs.S3TypeDataFrame : "s3_type_dataframe",
  function.FunctionType  : "function_type",
}


data_table_lookup = {
  core.Geom          : "geom",
  core.Owner         : "owner",
  core.Grower        : "grower",
  core.Field         : "field",
  local.LocalValue    : "local_value",
  core.GeoSpatialKey : "geospatial_key",
  core.TemporalKey   : "temporal_key",
  #inputs.S3Output     : "s3_output",
  inputs.S3Object     : "s3_object",
  function.Function   : "function",
  execution.Execution : "execution",
}

id_table_lookup = type_table_lookup.copy()
id_table_lookup.update(data_table_lookup) # type: ignore

key_table_lookup = {
  local.Planting     : ("planting", local.PlantingKey),
  local.Harvest      : ("harvest",  local.HarvestKey),
  local.CropProgress : ("crop_progress", local.CropProgressKey),
  inputs.S3ObjectKey  : ("s3_object_key", inputs.S3ObjectKey),
  inputs.S3TypeDataFrame : ("s3_type_dataframe", inputs.S3TypeDataFrame),
  function.LocalParameter : ("local_parameter", function.LocalParameter),
  function.HTTPParameter     : ("http_parameter", function.HTTPParameter),
  function.S3InputParameter  : ("s3_input_parameter", function.S3InputParameter),
  function.S3OutputParameter : ("s3_output_parameter", function.S3OutputParameter),
  function.KeywordParameter : ("keyword_parameter", function.KeywordParameter),
  execution.ExecutionKey     : ("execution_key", execution.ExecutionKey),
  execution.LocalArgument    : ("local_argument", execution.LocalArgument),
  execution.HTTPArgument     : ("http_argument", execution.HTTPArgument),
  execution.KeywordArgument  : ("keyword_argument", execution.KeywordArgument),
  execution.S3InputArgument  : ("s3_input_argument", execution.S3InputArgument),
  execution.S3OutputArgument : ("s3_output_argument", execution.S3OutputArgument),
}
