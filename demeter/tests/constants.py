# from pandas import read_sql_query

# sql = """
# SELECT *
# FROM pg_catalog.pg_tables
# WHERE schemaname = %(schema_name)s;
# """
# df = read_sql_query(sql, c, params={"schema_name": "dem_test"})
# TABLES_LIST = list(df["tablename"].unique())

TABLES_LIST = [
    "act",
    "crop_type",
    "field",
    "field_trial",
    "geom",
    "grouper",
    "nutrient_type",
    "plot",
    "observation",
    "observation_type",
    "organization",
    "unit_type",
]
