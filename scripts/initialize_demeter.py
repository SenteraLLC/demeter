import argparse
import subprocess
from os.path import (
    dirname,
    join,
    realpath,
)
from tempfile import NamedTemporaryFile

from dotenv import load_dotenv  # type: ignore
from psycopg2.extensions import AsIs
from sqlalchemy.engine import Connection

from demeter.db import getConnection


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
        print(f"A schema of name {schema_name} already exists in this database.")
        if not drop_existing:
            print("Change `drop_existing` to True if you'd like to drop it.")
            return False
        else:
            print(
                "`drop_existing` is True. The existing schema will be dropped and overwritten."
            )
            stmt = """DROP SCHEMA IF EXISTS %s CASCADE;"""
            params = AsIs(schema_name)
            conn.execute(stmt, params)

    # make temporary SQL file and overwrite schema name lines
    root_dir = realpath(join(dirname(__file__), ".."))
    with NamedTemporaryFile() as tmp:
        with open(join(root_dir, "schema.sql"), "r") as schema_f:
            schema_sql = schema_f.read()
            # Change schema name in SQL script if needed.
            if schema_name != "test_demeter":
                schema_sql = schema_sql.replace("test_demeter", schema_name)
        tmp.write(schema_sql.encode())  # Writes SQL script to a temp file
        host = conn.engine.url.host
        username = conn.engine.url.username
        password = conn.engine.url.password
        database = conn.engine.url.database
        psql = f'PGPASSWORD={password} psql -h {host} -U {username} -f "{tmp.name}" {database}'
        subprocess.call(psql, shell=True)

    return True


if __name__ == "__main__":

    c = load_dotenv()

    parser = argparse.ArgumentParser(description="Initialize Demeter instance.")

    parser.add_argument(
        "--schema_name",
        type=str,
        help="Schema name to use for new Demeter instance.",
        required=True,
    )

    parser.add_argument(
        "--database_host",
        type=str,
        help="Host of database to query/change; can be 'AWS' or 'LOCAL'.",
        default="LOCAL",
    )

    parser.add_argument(
        "--drop_existing",
        type=bool,
        help="Should the schema be re-created if it exists?",
        default=False,
    )

    args = parser.parse_args()
    schema_name = args.schema_name
    database_host = args.database_host
    drop_existing = args.drop_existing

    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"

    if database_host == "AWS":
        conn = getConnection(env_name="DEMETER_AWS")
    else:
        conn = getConnection(env_name="DEMETER")

    _ = initialize_demeter_instance(conn, schema_name, drop_existing)
