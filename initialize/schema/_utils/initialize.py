import logging
import subprocess
from os.path import (
    dirname,
    join,
    realpath,
)
from tempfile import NamedTemporaryFile
from typing import Callable

from psycopg2.extensions import AsIs
from sqlalchemy.engine import Connection

from .format import (
    format_demeter_schema_sql,
    format_raster_schema_sql,
    format_weather_schema_sql,
)

SQL_FUNCTIONS = {
    "DEMETER": format_demeter_schema_sql,
    "RASTER": format_raster_schema_sql,
    "WEATHER": format_weather_schema_sql,
}

SQL_FNAMES = {
    "DEMETER": "schema_demeter.sql",
    "RASTER": "schema_raster.sql",
    "WEATHER": "schema_weather.sql",
}


def _check_exists_schema(conn: Connection, schema_name: str) -> bool:
    """Checks to see if a schema of name `schema_name` exists in the connected db.

    If the schema exists, returns True. Else, returns False.
    """
    with conn.connection.cursor() as cursor:
        # check if the given schema name already exists
        stmt = """
            select * from information_schema.schemata
            where schema_name = %(schema_name)s
        """
        cursor = conn.connection.cursor()
        cursor.execute(stmt, {"schema_name": schema_name})
        results = cursor.fetchall()

    if len(results) > 0:
        return True
    else:
        return False


def _drop_schema(conn: Connection, schema_name: str):
    """Drops schema of name `schema_name` from connected db.

    Requires superuser connection.
    """
    stmt = """DROP SCHEMA IF EXISTS %s CASCADE;"""
    params = AsIs(schema_name)
    conn.execute(stmt, params)


def _maybe_initialize_schema(
    conn: Connection,
    schema_name: str,
    sql_fname: str,
    sql_function: Callable = None,
    drop_existing: bool = False,
) -> False:
    """Initializes a schema defined in `sql_fname` in the connected database.

    This function checks to see if a schema sharing the same name already exists in
    the database server. If so, it will drop that schema if `drop_existing` is True.
    If there is no existing schema, the SQL file is loaded in as a temporary file from
    `sql_fname` and, if applicable, a `sql_function` is applied to format the given SQL
    statements. Then, the SQL statement is executed using a subprocess for the connected
    database.

    Args:
        conn: Connection to `demeter` database server.
        schema_name (str): Name of schema to initialize.
        sql_fname (str): Location of SQL schema file.

        sql_function (Callable): Function that takes `schema_name` (str) and `schema_sql` (str)
            and performs the necessary formatting to ready the SQL text for execution. This step
            us to customize this initialization function across schemas.

        drop_existing (bool): If True, an existing schema of `schema_name` should be dropped and
            then recreated. Else, an existing schema will be untouched.

    Returns True if the schema was initialized. Returns False if the schema already exists and was not
        reinitialized.
    """
    exists = _check_exists_schema(conn, schema_name)

    # if the schema exists already, drop existing if `drop_existing` is True, else do nothing
    if exists:
        logging.info(
            "A schema of name %s already exists in this database.", schema_name
        )
        if drop_existing is False:
            logging.info(
                "No further action will be taken as no `--drop_existing` flag was passed.\n"
                "Add `--drop_existing` flag to command call if you would like to re-initialize the schema."
            )
            return False
        else:
            logging.info(
                "`drop_existing` is True. The existing schema will be dropped and overwritten."
            )
            _drop_schema(conn, schema_name)

    # make temporary SQL file, adjust as needed, and execute SQL
    with NamedTemporaryFile() as tmp:
        with open(sql_fname, "r") as schema_f:
            schema_sql = schema_f.read()

            # Change schema name in SQL script if needed.
            if sql_function is not None:
                schema_sql = sql_function(schema_name, schema_sql)

        tmp.write(schema_sql.encode())  # Writes SQL script to a temp file
        tmp.flush()  # Push file contents so they are accessible
        host = conn.engine.url.host
        username = conn.engine.url.username
        password = conn.engine.url.password
        database = conn.engine.url.database
        port = conn.engine.url.port
        psql = f'PGPASSWORD={password} psql -h {host} -p {port} -U {username} -f "{tmp.name}" {database}'
        subprocess.call(psql, shell=True)

    return True


def initialize_schema_type(
    conn: Connection, schema_name: str, schema_type: str, drop_existing: bool = False
) -> bool:
    """Initializes schema with given `schema_name` using database connection.

    Returns True if schema was successfully initialized.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter database where schema should be created.
        schema_name (str): Name to give schema.
        drop_existing (bool): Indicates whether or not an existing schema called `schema_name` should be dropped if exists.
    """

    assert (
        schema_type in SQL_FNAMES.keys()
    ), f"`schema_type` = {schema_type} not implemented."

    file_dir = realpath(join(dirname(__file__), ".."))
    sql_fname = join(file_dir, "_sql", SQL_FNAMES[schema_type])

    sql_function = SQL_FUNCTIONS[schema_type]

    return _maybe_initialize_schema(
        conn,
        schema_name=schema_name,
        sql_fname=sql_fname,
        sql_function=sql_function,
        drop_existing=drop_existing,
    )
