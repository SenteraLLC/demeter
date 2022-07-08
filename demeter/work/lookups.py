from ..db.lookup_types import TableLookup, KeyTableLookup

from .types import *

type_table_lookup : TableLookup = { }

data_table_lookup : TableLookup = {
  Execution : "execution",
}

from ..db.lookup_types import sumMappings

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup : KeyTableLookup = {
  ExecutionKey     : ("work_key", ExecutionKey),
  LocalArgument    : ("data_argument", LocalArgument),
  HTTPArgument     : ("http_argument", HTTPArgument),
  KeywordArgument  : ("keyword_argument", KeywordArgument),
  S3InputArgument  : ("s3_input_argument", S3InputArgument),
  S3OutputArgument : ("s3_output_argument", S3OutputArgument),
}

