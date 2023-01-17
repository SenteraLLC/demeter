from ..db._lookup_types import TableLookup, sumMappings
from ._types import (
    Execution,
    ExecutionKey,
    HTTPArgument,
    KeywordArgument,
    ObservationArgument,
    S3InputArgument,
    S3OutputArgument,
)

type_table_lookup: TableLookup = {}
data_table_lookup: TableLookup = {
    Execution: "execution",
}


id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    ExecutionKey: "execution_key",
    ObservationArgument: "observation_argument",
    HTTPArgument: "http_argument",
    KeywordArgument: "keyword_argument",
    S3InputArgument: "s3_input_argument",
    S3OutputArgument: "s3_output_argument",
}
