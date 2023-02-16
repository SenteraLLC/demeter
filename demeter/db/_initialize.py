import logging
import subprocess
from os import getenv
from os.path import (
    dirname,
    join,
    realpath,
)
from tempfile import NamedTemporaryFile

from psycopg2.extensions import AsIs
from sqlalchemy.engine import Connection


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

    # check if the given schema name already exists
    stmt = """
        select * from information_schema.schemata
        where schema_name = %(schema_name)s
    """
    cursor = conn.connection.cursor()
    cursor.execute(stmt, {"schema_name": schema_name})
    results = cursor.fetchall()

    # if the schema exists already, drop existing if `drop_existing` is True, else do nothing
    if len(results) > 0:
        logging.info(
            "A schema of name %s already exists in this database.", schema_name
        )
        if drop_existing is False:
            logging.info(
                "No further action will be taken as no `--drop_existing` flag was passed."
            )
            logging.info(
                "Add `--drop_existing` flag to command call if you would like to re-initialize the schema."
            )
            return False
        else:
            logging.info(
                "`drop_existing` is True. The existing schema will be dropped and overwritten."
            )
            stmt = """DROP SCHEMA IF EXISTS %s CASCADE;"""
            params = AsIs(schema_name)
            conn.execute(stmt, params)

    # make temporary SQL file and overwrite schema name lines
    root_dir = realpath(join(dirname(__file__), "..", ".."))
    with NamedTemporaryFile() as tmp:
        with open(join(root_dir, "schema.sql"), "r") as schema_f:
            schema_sql = schema_f.read()
            # Change schema name in SQL script if needed.
            if schema_name != "test_demeter":
                schema_sql = schema_sql.replace("test_demeter", schema_name)

            # Add user passwords
            schema_sql = schema_sql.replace(
                "demeter_user_password", getenv("demeter_user_password")
            )
            schema_sql = schema_sql.replace(
                "demeter_ro_user_password", getenv("demeter_ro_user_password")
            )

        tmp.write(schema_sql.encode())  # Writes SQL script to a temp file
        tmp.flush()  # Push file contents so they are accessible
        host = conn.engine.url.host
        username = conn.engine.url.username
        password = conn.engine.url.password
        database = conn.engine.url.database
        port = conn.engine.url.port
        psql = f'PGPASSWORD={password} psql -h {host} -p {port} -U {username} -f "{tmp.name}" {database}'
        logging.info("%s", psql)
        subprocess.call(psql, shell=True)

    return True
