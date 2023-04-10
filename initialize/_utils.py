import logging
import sys

import click


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
