from ..db._lookup_types import TableLookup
from ._types import *

type_table_lookup: TableLookup = {}

data_table_lookup: TableLookup = {
    Execution: "execution",
}

from ..db._lookup_types import sumMappings

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    ExecutionKey: "execution_key",
    ObservationArgument: "observation_argument",
    HTTPArgument: "http_argument",
    KeywordArgument: "keyword_argument",
    S3InputArgument: "s3_input_argument",
    S3OutputArgument: "s3_output_argument",
}
