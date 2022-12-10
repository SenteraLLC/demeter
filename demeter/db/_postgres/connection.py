import os
from typing import Any, Optional

from typing import Type

import psycopg2
import psycopg2.extras
from psycopg2.extensions import adapt
from psycopg2.extensions import connection
from psycopg2.extensions import register_adapter


def getEnv(
    name: str, default: Optional[Any] = None, is_required: bool = False
) -> Optional[str]:
    v = os.environ.get(name, default)
    if is_required and v is None:
        raise Exception(f"Environment variable for '{name}' not set")
    return v


def getConnection(
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    host_key: str = "DEMETER_PG_HOST",
    port_key: str = "DEMETER_PG_PORT",
    pw_key: str = "DEMETER_PG_PASSWORD",
    user_key: str = "DEMETER_PG_USER",
    options_key: str = "DEMETER_PG_OPTIONS",
    db_key: str = "DEMETER_PG_DATABASE",
) -> connection:
    register_adapter(set, lambda s: adapt(list(s)))  # type: ignore

    host = getEnv(host_key, "localhost")
    port = getEnv(port_key, 5432)
    password = getEnv(pw_key)
    user = getEnv(user_key, "postgres")
    options = getEnv(options_key, "")
    database = getEnv(db_key, "postgres")
    return psycopg2.connect(
        host=host,
        port=port,
        password=password,
        options=options,
        database=database,
        user=user,
        cursor_factory=cursor_type,
    )
