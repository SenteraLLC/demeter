import os
from ast import literal_eval
from typing import (
    Any,
    Dict,
    Optional,
    Type,
)

import psycopg2
import psycopg2.extras
from psycopg2.extensions import (
    adapt,
    connection,
    register_adapter,
)
from sqlalchemy.engine import (
    URL,
    Connection,
    create_engine,
)
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder


def getEnv(
    name: str,
    default: Optional[Any] = None,
    is_required: bool = False,
    required_keys: list = None,
) -> Optional[str]:
    v = os.environ.get(name, default)

    if is_required and v is None:
        raise Exception(
            f"Environment variable for {name} not set.\nDid your app include `load_dotenv()`?\n"
            + f"Does the {name} environment variable exist?"
        )
    if required_keys is not None:
        try:
            v_dict = literal_eval(v)
        except SyntaxError as e:
            raise SyntaxError(
                f"\nCannot perform a literal_eval on {name} environment variable\n{e}"
            )
        assert set(required_keys).issubset(
            list(v_dict.keys())
        ), f"These keys must be present in {name}: {required_keys}"
    return v


def getConnection(
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> Connection:
    return getEngine(
        env_name=env_name,
        cursor_type=cursor_type,
        dialect=dialect,
        ssh_env_name=ssh_env_name,
    ).connect()


def check_ssh_env(
    db_meta: Dict,
    ssh_env_name: str = None,
) -> Dict:
    if ssh_env_name:
        cols_ssh = [
            "ssh_address_or_host",
            "ssh_username",
            "ssh_pkey",
            "remote_bind_address",
        ]
        ssh_meta = literal_eval(
            getEnv(ssh_env_name, is_required=True, required_keys=cols_ssh)
        )
        ssh_address_or_host = ssh_meta["ssh_address_or_host"]
        ssh_username = ssh_meta["ssh_username"]
        ssh_private_key = ssh_meta["ssh_pkey"]
        remote_bind_address = ssh_meta["remote_bind_address"]
        db_meta["port"] = ssh_bind_port(
            ssh_address_or_host, ssh_username, ssh_private_key, remote_bind_address
        )
    return db_meta


def getExistingSearchPath(
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> str:
    cols_env = ["host", "port", "username", "password", "database", "schema_name"]
    db_meta = check_ssh_env(
        literal_eval(getEnv(env_name, is_required=True, required_keys=cols_env)),
        ssh_env_name,
    )  # make into dictionary
    connect_args = {
        "cursor_factory": cursor_type,
    }
    #  Can't use getEngine() because getEngine() needs getExistingSearchPath()
    url_object = URL.create(
        dialect,
        host=db_meta["host"],
        port=db_meta["port"],
        username=db_meta["username"],
        password=db_meta["password"],
        database=db_meta["database"],
    )
    with create_engine(url_object, connect_args=connect_args).connect() as conn:
        stmt = """
        SELECT rs.setconfig
        FROM pg_db_role_setting rs
        LEFT JOIN pg_roles r ON r.oid = rs.setrole
        WHERE r.rolname = '%(username)s';
        """
        cursor = conn.connection.cursor()
        args = {"username": db_meta["username"]}
        cursor.execute(stmt, args)
        results = cursor.fetchall()
    search_path = (
        results[0].setconfig[0].replace(" ", "") if len(results) != 0 else None
    )
    return search_path


def getEngine(
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
) -> Connection:
    cols_env = ["host", "port", "username", "password", "database", "schema_name"]
    db_meta = check_ssh_env(
        literal_eval(getEnv(env_name, is_required=True, required_keys=cols_env)),
        ssh_env_name,
    )  # make into dictionary

    # Get existing search_path and combine with whatever is in db_meta["schema_name"] (if anything)
    search_path = getExistingSearchPath(env_name, cursor_type, dialect, ssh_env_name)
    if search_path is None:
        search_path_list = []
    else:
        search_path_list = search_path.split("=")[-1].split(",")
    if "schema_name" in db_meta.keys():
        if db_meta["schema_name"] not in search_path_list:
            # ensure "public" is in search path no matter the initial order; remove "public", then add it back
            search_path_list.remove(
                "public"
            ) if "public" in search_path_list else search_path_list
            # prepend schema_name to search_path
            if len(search_path_list) == 0:
                search_path = f"search_path={db_meta['schema_name']},public"
            else:
                search_path = f"search_path={db_meta['schema_name']},{','.join(search_path_list)},public"
    connect_args = {
        "options": f"-c {search_path}",  # overwrites search path, but gets according to getExistingSearchPath()
        "cursor_factory": cursor_type,
    }

    url_object = URL.create(
        dialect,
        host=db_meta["host"],
        port=db_meta["port"],
        username=db_meta["username"],
        password=db_meta["password"],
        database=db_meta["database"],
    )
    return create_engine(url_object, connect_args=connect_args)


def ssh_bind_port(
    ssh_address_or_host: str,
    ssh_username: str,
    ssh_private_key: str,
    remote_bind_address: str,
) -> str:
    """Use SSHTunnelForwarder to bind host to a local port."""
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


def getSession(
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
    ssh_env_name: str = None,
):
    return sessionmaker(
        getEngine(
            env_name=env_name,
            cursor_type=cursor_type,
            dialect=dialect,
            ssh_env_name=ssh_env_name,
        ).connect()
    )


def getConnection_psycopg2(
    env_name: str = "TEST_DEMETER",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
) -> connection:
    register_adapter(set, lambda s: adapt(list(s)))  # type: ignore - not sure what this does?

    db_meta = literal_eval(getEnv(env_name))  # make into dictionary
    schema_name = (
        db_meta["schema_name"] + "," if "schema_name" in db_meta.keys() else ""
    )
    options = "-c search_path={}public".format(schema_name)

    return psycopg2.connect(
        host=db_meta["host"],
        port=db_meta["port"],
        password=db_meta["password"],
        options=options,
        database=db_meta["database"],
        username=db_meta["username"],
        cursor_factory=cursor_type,
    )
