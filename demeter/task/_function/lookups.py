from ...db._lookup_types import TableLookup
from .types import *

type_table_lookup: TableLookup = {
    FunctionType: "function_type",
}

data_table_lookup: TableLookup = {
    Function: "function",
}

from ...db._lookup_types import sumMappings

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    LocalParameter: "local_parameter",
    HTTPParameter: "http_parameter",
    S3InputParameter: "s3_input_parameter",
    S3OutputParameter: "s3_output_parameter",
    KeywordParameter: "keyword_parameter",
}
