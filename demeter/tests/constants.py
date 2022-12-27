# from pandas import read_sql_query

# sql = """
# SELECT *
# FROM pg_catalog.pg_tables
# WHERE schemaname = %(schema_name)s;
# """
# df = read_sql_query(sql, c, params={"schema_name": "dem_test"})
# TABLES_LIST = list(df["tablename"].unique())

TABLES_LIST = [
    "geom",
    "field_group",
    "field",
    "crop_type",
    "planting",
    "observation_type",
    "harvest",
    "unit_type",
    "s3_type",
    "s3_type_dataframe",
    "http_type",
    "observation_parameter",
    "function_type",
    "s3_object",
    "function",
    "geospatial_key",
    "observation",
    "act",
    "s3_object_key",
    "temporal_key",
    "http_parameter",
    "s3_input_parameter",
    "s3_output_parameter",
    "keyword_parameter",
    "execution",
    "execution_key",
    "observation_argument",
    "http_argument",
    "s3_input_argument",
    "keyword_argument",
    "s3_output_argument",
    "published_workflow",
    "node",
    "node_raster",
    "root",
    "node_ancestry",
]
