import logging
import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import Callable, Iterable

import click
from pandas import DataFrame
from psycopg2.extensions import AsIs
from sqlalchemy.engine import Connection


def check_schema_users(conn: Connection, user_list: Iterable[str]) -> Iterable[str]:
    """Checks the database and ensures that all users in `user_list` exist.

    This function will raise an AssertionError if not all users in `user_list` exist
    in the database, but, otherwise, does nothing.
    """
    if not isinstance(user_list, tuple):
        user_list = tuple(user_list)

    with conn.connection.cursor() as cursor:
        # check if the given schema name already exists
        stmt = """
            select rolname from pg_roles
            where rolname in %(user_list)s
        """
        cursor = conn.connection.cursor()
        cursor.execute(stmt, {"user_list": user_list})
        df_results = DataFrame(cursor.fetchall())

        if len(df_results) > 0:
            existing_users = df_results["rolname"].to_list()
            missing_users = []
            for user in user_list:
                if user not in existing_users:
                    missing_users += [user]
        else:
            missing_users = user_list

    msg = (
        f"The following users have not yet been created: {missing_users}.\n"
        "Please run `python3 -m initialize.users`."
    )
    assert len(missing_users) == 0, msg


def check_and_format_db_connection_args(host: str, env: str, superuser: bool = False):
    """Verfies AWS connection (if applicable) and formats connection arguments for `getConnection()`.

    Args:
        host (str): Host of database to query/change; can be 'AWS' or 'LOCAL'.
        env (str): Database instance to query/change; can be 'DEV' or 'PROD'.

        superuser (bool): If True, the superuser permissions will be used. Otherwise,
            `demeter_user` credentials will be used (i.e., read and write). Defaults to False.
    """
    assert host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"
    assert env in ["DEV", "PROD"], "`database_env` can be 'DEV' or 'PROD'"

    if host == "AWS":
        if click.confirm(
            "Are you sure you want to tunnel to AWS database?", default=False
        ):
            logging.info("Connecting to AWS database instance.")
        else:
            sys.exit()

    ssh_env_name = f"SSH_DEMETER_{host}" if host == "AWS" else None

    if superuser:
        database_env_name = f"DEMETER-{env}_{host}_SUPER"
    else:
        database_env_name = f"DEMETER-{env}_{host}"

    return database_env_name, ssh_env_name


def confirm_user_choice(flag: bool, question: str, no_response: str):
    """Takes `flag` is True, a y/N `question` is posed to the user to be confirmed on the command line.

    If the user responds "N", `no_response` is printed to the command line.

    Args:
        flag (bool): Bool argument passed to a CLI program.
        question (str): Question to be posed to the user if `flag` is True.
        no_response (str): Logged response if the user responds 'N'.
    """
    if flag:
        if click.confirm(question, default=False):
            pass
        else:
            logging.info(no_response)
            flag = False
    return flag


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


def initialize_schema(
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
