from ...db._lookup_types import TableLookup, sumMappings
from .._function.types import (
    Function,
    FunctionType,
    HTTPParameter,
    KeywordParameter,
    ObservationParameter,
    S3InputParameter,
    S3OutputParameter,
)

type_table_lookup: TableLookup = {
    FunctionType: "function_type",
}

data_table_lookup: TableLookup = {
    Function: "function",
}

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    ObservationParameter: "observation_parameter",
    HTTPParameter: "http_parameter",
    S3InputParameter: "s3_input_parameter",
    S3OutputParameter: "s3_output_parameter",
    KeywordParameter: "keyword_parameter",
}
