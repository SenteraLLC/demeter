import os
from ast import literal_eval
from socket import gethostbyname
from typing import (
    Any,
    Dict,
    Optional,
    Type,
)

import psycopg2
import psycopg2.extras
from sqlalchemy.engine import (
    URL,
    Connection,
    Engine,
    create_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sshtunnel import SSHTunnelForwarder

DB_DICT_KEYS = ("host", "port", "username", "password", "database")
SSH_DICT_KEYS = (
    "ssh_address_or_host",
    "ssh_username",
    "ssh_pkey",
    "remote_bind_address",
)


def _check_and_get_env_dictionary(
    env_name: str,
    default: Optional[Any] = None,
    is_required: bool = False,
    required_keys: list = None,
) -> dict:
    """
    Get the environment dictionary variable for a given environment name and check that it is valid.

    Args:
        env_name (str): Environment key.
        default (Optional[Any], optional): Default environment value; only used if environment key is
        not set. Defaults to None.
        is_required (bool, optional): Whether environment value is required (if so, Exception will be raised if
        environment value is `None`). Defaults to False.
        required_keys (list, optional): The keys that are required to exist in the environment value; only useful for
        environment values expressed as a json format (with key/value pairs). Defaults to None.

    Raises:
        Exception: If environment value is `None` and is_required is `True`.
        SyntaxError: If `required_keys` is `True` and environment value cannot be parsed as a json/dict.

    Returns:
        dict: Environment dictionary.
    """
    env_value = os.environ.get(env_name, default)

    if is_required and env_value is None:
        raise Exception(
            f"Environment variable for {env_name} not set.\nDid your app include `load_dotenv()`?\n"
            + f"Does the {env_name} environment variable exist?"
        )
    if required_keys is not None:
        try:
            v_dict = literal_eval(env_value)
        except SyntaxError as e:
            raise SyntaxError(
                f"\nCannot perform a literal_eval on {env_name} environment variable\n{e}"
            )
        assert set(required_keys).issubset(
            list(v_dict.keys())
        ), f"These keys must be present in {env_name}: {required_keys}"
    env_dict = literal_eval(env_value)
    if env_dict.get("host") == "host.docker.internal":
        env_dict["host"] = gethostbyname("host.docker.internal")
    return literal_eval(env_value)


def _get_ssh_bind_port(
    ssh_address_or_host: str,
    ssh_username: str,
    ssh_private_key: str,
    remote_bind_address: str,
) -> str:
    """
    Use SSHTunnelForwarder to bind host to a local port.

    Args:
        ssh_address_or_host (str): IP or hostname of REMOTE GATEWAY. It may be a two-element tuple (str, int)
        representing IP and port respectively, or a str representing the IP address only.
        ssh_username (str): Username to authenticate as in REMOTE SERVER.
        ssh_private_key (str): Private key file name (str) to obtain the public key from or a public key
        (paramiko.pkey.PKey)
        remote_bind_address (str): The IP of the remote side of the tunnel.

    Returns:
        str: Local port after binding SHH host.
    """
    server = SSHTunnelForwarder(
        ssh_address_or_host=(ssh_address_or_host, 22),
        ssh_username=ssh_username,
        ssh_private_key=ssh_private_key,
        remote_bind_address=(remote_bind_address, 5432),
        set_keepalive=15,
    )
    server.daemon_forward_servers = True
    server.start()
    local_port = str(server.local_bind_port)
    return local_port


def _maybe_setup_ssh_env(
    db_fields: Dict,
    ssh_env_name: str = None,
) -> Dict:
    """
    Checks for an SSH environment key, and if exists, overwrites `db_fields["port"]` with the binded SSH port.

    Args:
        db_fields (Dict): Key/value pairs of necessary database connection arguments. See
        [sqlalchemy.engine.URL.creat()](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.engine.URL.create)
        for available arguments.
        ssh_env_name (str, optional): The environment key that holds credentials for an SSH tunnel; setting to `None`
        will establish a connection without use of an SSH tunnel. Defaults to None.

    Returns:
        Dict: Key/value pairs of database connection arguments, adapted for SSH tunneling if available.
    """
    if ssh_env_name:
        ssh_meta = _check_and_get_env_dictionary(
            ssh_env_name, is_required=True, required_keys=SSH_DICT_KEYS
        )

        ssh_address_or_host = ssh_meta["ssh_address_or_host"]
        ssh_username = ssh_meta["ssh_username"]
        ssh_private_key = ssh_meta["ssh_pkey"]
        remote_bind_address = ssh_meta["remote_bind_address"]

        db_fields["port"] = _get_ssh_bind_port(
            ssh_address_or_host, ssh_username, ssh_private_key, remote_bind_address
        )
    return db_fields


def getExistingSearchPath(
    env_name: str,
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> str:
    """
    Gets existing `search_path` of database connection based on connecting user.

    Args:
        env_name (str): Environment key.
        cursor_type (Type[psycopg2.extensions.cursor], optional): Psycopg2 cursor type to use. Defaults to
        `psycopg2.extras.NamedTupleCursor`.
        dialect (str, optional): Database dialect to be used in creation of database connection URL. Defaults to
        "postgresql+psycopg2".
        ssh_env_name (str, optional): The environment key that holds credentials for an SSH tunnel; setting to `None`
        will establish a connection without use of an SSH tunnel. Defaults to None.

    Returns:
        str: `search_path` for passed database connection (e.g., "search_path=demeter,weather,public").
    """
    # Organize connection details
    db_env_fields = _check_and_get_env_dictionary(
        env_name, is_required=True, required_keys=DB_DICT_KEYS
    )
    db_fields = _maybe_setup_ssh_env(db_env_fields, ssh_env_name)

    connect_args = {
        "cursor_factory": cursor_type,
    }

    # Can't use getEngine() because getEngine() needs getExistingSearchPath()
    url_object = URL.create(
        dialect,
        host=db_fields["host"],
        port=db_fields["port"],
        username=db_fields["username"],
        password=db_fields["password"],
        database=db_fields["database"],
    )
    with create_engine(url_object, connect_args=connect_args).connect() as conn:
        stmt = """
        SELECT rs.setconfig
        FROM pg_db_role_setting rs
        LEFT JOIN pg_roles r ON r.oid = rs.setrole
        WHERE r.rolname = %(username)s;
        """
        cursor = conn.connection.cursor()
        args = {"username": db_fields["username"]}
        cursor.execute(stmt, args)
        results = cursor.fetchall()
    search_path = (
        results[0].setconfig[0].replace(" ", "") if len(results) != 0 else None
    )
    return search_path


def getEngine(
    env_name: str,
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> Engine:
    """
    Establish a database engine (via sqlalchemy).

    Search path is assumed to be the default "search_path" for the given user unless "search_path" is set
    in the passed `env_name` permission dictionary. Any "search_path" value in the permission dictionary
    will overwrite the default search path for created connection.

    Args:
        env_name (str): Environment key.
        cursor_type (Type[psycopg2.extensions.cursor], optional): Psycopg2 cursor type to use. Defaults to
        `psycopg2.extras.NamedTupleCursor`.
        dialect (str, optional): Database dialect to be used in creation of database connection URL. Defaults to
        "postgresql+psycopg2".
        ssh_env_name (str, optional): The environment key that holds credentials for an SSH tunnel; setting to `None`
        will establish a connection without use of an SSH tunnel. Defaults to None.

    Returns:
        Engine: The sqlalchemy database engine.
    """
    # Organize connection details
    db_env_fields = _check_and_get_env_dictionary(
        env_name, is_required=True, required_keys=DB_DICT_KEYS
    )
    db_fields = _maybe_setup_ssh_env(db_env_fields, ssh_env_name)

    if "search_path" in db_fields.keys():
        search_path_arg = db_fields["search_path"]
        search_path_list = search_path_arg.split(",")

        # Ensure "public" is in search path and is last
        if "public" in search_path_list:
            search_path_list.remove("public")
        search_path_list += ["public"]

        search_path_str = ",".join(search_path_list)
        search_path = f"search_path={search_path_str}"

    else:  # get existing search_path and combine with whatever is in db_fields["schema_name"] (if anything)
        search_path = getExistingSearchPath(
            env_name, cursor_type, dialect, ssh_env_name
        )

    connect_args = {
        "options": f"-c {search_path}",  # overwrites search path, but gets according to getExistingSearchPath()
        "cursor_factory": cursor_type,
    }
    if search_path is None:
        if "options" in connect_args:
            del connect_args["options"]

    url_object = URL.create(
        dialect,
        host=db_fields["host"],
        port=db_fields["port"],
        username=db_fields["username"],
        password=db_fields["password"],
        database=db_fields["database"],
    )

    return create_engine(url_object, connect_args=connect_args)


def getConnection(
    env_name: str,
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> Connection:
    """
    Establish a database connection (via sqlalchemy).

    Search path is assumed to be the default "search_path" for the given user unless "search_path" is set
    in the passed `env_name` permission dictionary. Any "search_path" value in the permission dictionary
    will overwrite the default search path for created connection.


    Args:
        env_name (str): Environment key.
        cursor_type (Type[psycopg2.extensions.cursor], optional): Psycopg2 cursor type to use. Defaults to
        `psycopg2.extras.NamedTupleCursor`.
        dialect (str, optional): Database dialect to be used in creation of database connection URL. Defaults to
        "postgresql+psycopg2".
        ssh_env_name (str, optional): The environment key that holds credentials for an SSH tunnel; setting to `None`
        will establish a connection without use of an SSH tunnel. Defaults to None.

    Returns:
        Connection: The sqlalchemy database connection.
    """
    return getEngine(
        env_name=env_name,
        cursor_type=cursor_type,
        dialect=dialect,
        ssh_env_name=ssh_env_name,
    ).connect()


def getSession(
    env_name: str,
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> Session:
    """
    Establish a database session (via sqlalchemy).

    Args:
        env_name (str): Environment key.
        cursor_type (Type[psycopg2.extensions.cursor], optional): Psycopg2 cursor type to use. Defaults to
        `psycopg2.extras.NamedTupleCursor`.
        dialect (str, optional): Database dialect to be used in creation of database connection URL. Defaults to
        "postgresql+psycopg2".
        ssh_env_name (str, optional): The environment key that holds credentials for an SSH tunnel; setting to `None`
        will establish a connection without use of an SSH tunnel. Defaults to None.

    Returns:
        Session: The sqlalchemy database session.
    """
    return sessionmaker(
        getEngine(
            env_name=env_name,
            cursor_type=cursor_type,
            dialect=dialect,
            ssh_env_name=ssh_env_name,
        ).connect()
    )
