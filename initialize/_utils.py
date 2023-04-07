import logging
import sys
from typing import Any, Iterable

import click
from pandas import DataFrame
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


def check_and_format_db_connection_args(host: str, env: str):
    """DOCSTRING"""

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
    database_env_name = f"DEMETER-{env}_{host}_SUPER"

    return database_env_name, ssh_env_name


def get_flag_as_bool(arg: Any) -> bool:
    """DOCSTRING"""
    if arg:
        return True
    else:
        return False


def confirm_user_choice(flag: bool, question: str, no_response: str):
    """DOCSTRING"""
    if flag:
        if click.confirm(question, default=False):
            pass
        else:
            logging.info(no_response)
            flag = False
    return flag
