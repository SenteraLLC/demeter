from os.path import (
    dirname,
    join,
    realpath,
)

from sqlalchemy.engine import Connection

from demeter._version import __version__
from initialize._utils import initialize_schema


def _format_raster_schema_sql(schema_name: str, schema_sql: str) -> str:
    """Formats `demeter` schema SQL file with new `schema_name` and version."""

    schema_sql = schema_sql.replace("v0.0.0", "v" + __version__)

    return schema_sql


def initialize_raster_schema(conn: Connection, drop_existing: bool = False) -> bool:
    """Initializes raster schema using database connection.

    Returns True if schema was successfully initialized.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter database where schema should be created.
        drop_existing (bool): Indicates whether or not an existing schema called `raster` should be dropped if exists.
    """

    file_dir = realpath(join(dirname(__file__), ".."))
    sql_fname = join(file_dir, "sql", "schema_raster.sql")

    return initialize_schema(
        conn,
        schema_name="raster",
        sql_fname=sql_fname,
        sql_function=_format_raster_schema_sql,
        drop_existing=drop_existing,
    )
