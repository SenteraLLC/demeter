from ...db.lookup_types import TableLookup, KeyTableLookup

from .types import *

type_table_lookup : TableLookup = {
  FunctionType  : "function_type",
}

data_table_lookup : TableLookup = {
  Function : "function",
}

from ...db.lookup_types import sumMappings

id_table_lookup : TableLookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup : KeyTableLookup = {
  LocalParameter : ("data_parameter", LocalParameter),
  HTTPParameter     : ("http_parameter", HTTPParameter),
  S3InputParameter  : ("s3_input_parameter", S3InputParameter),
  S3OutputParameter : ("s3_output_parameter", S3OutputParameter),
  KeywordParameter : ("keyword_parameter", KeywordParameter),
}

