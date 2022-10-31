from ...db._lookup_types import TableLookup
from .types import *

type_table_lookup: TableLookup = {
    HTTPType: "http_type",
    S3Type: "s3_type",
    S3TypeDataFrame: "s3_type_dataframe",
}

data_table_lookup: TableLookup = {
    S3Output: "s3_output",
    S3Object: "s3_object",
}

from ...db._lookup_types import sumMappings

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    S3ObjectKey: "s3_object_key",
    S3TypeDataFrame: "s3_type_dataframe",
}
