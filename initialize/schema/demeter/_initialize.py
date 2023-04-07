from os.path import (
    dirname,
    join,
    realpath,
)

from sqlalchemy.engine import Connection

from demeter._version import __version__
from initialize._utils import initialize_schema


def _format_demeter_schema_sql(schema_name: str, schema_sql: str) -> str:
    """Formats `demeter` schema SQL file with new `schema_name` and version."""

    if schema_name != "test_demeter":
        schema_sql = schema_sql.replace("test_demeter", schema_name)

    schema_sql = schema_sql.replace("v0.0.0", "v" + __version__)

    return schema_sql


def initialize_demeter_instance(
    conn: Connection, schema_name: str, drop_existing: bool = False
) -> bool:
    """Initializes Demeter schema with given `schema_name` using database connection.

    Returns True if schema was successfully initialized.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter database where schema should be created.
        schema_name (str): Name to give Demeter instance
        drop_existing (bool): Indicates whether or not an existing schema called `schema_name` should be dropped if exists.
    """

    file_dir = realpath(join(dirname(__file__), ".."))
    sql_fname = join(file_dir, "sql", "schema_demeter.sql")

    return initialize_schema(
        conn,
        schema_name=schema_name,
        sql_fname=sql_fname,
        sql_function=_format_demeter_schema_sql,
        drop_existing=drop_existing,
    )
