import os
from typing import Any, Optional

from typing import Type

import psycopg2
import psycopg2.extras
from psycopg2.extensions import adapt
from psycopg2.extensions import connection
from psycopg2.extensions import register_adapter
from sqlalchemy.engine import Connection, create_engine, URL


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
    db_key: str = "DEMETER_PG_DATABASE",
    dialect: str = "postgresql+psycopg2",
    schema_search_path: str = "test_demeter,public",
) -> Connection:
    connect_args = {
        "options": "-csearch_path={}".format(schema_search_path),
        "cursor_factory": cursor_type,
    }
    url_object = URL.create(
        dialect,
        host=getEnv(host_key, "localhost"),
        port=getEnv(port_key, 5432),
        username=getEnv(user_key, "postgres"),
        password=getEnv(pw_key),
        database=getEnv(db_key, "postgres"),
    )
    engine = create_engine(url_object, connect_args=connect_args)
    return engine.connect()


def getConnection_psycopg2(
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    host_key: str = "DEMETER_PG_HOST",
    port_key: str = "DEMETER_PG_PORT",
    pw_key: str = "DEMETER_PG_PASSWORD",
    user_key: str = "DEMETER_PG_USER",
    options_key: str = "DEMETER_PG_OPTIONS",
    db_key: str = "DEMETER_PG_DATABASE",
) -> connection:
    register_adapter(set, lambda s: adapt(list(s)))  # type: ignore - not sure what this does?

    return psycopg2.connect(
        host=getEnv(host_key, "localhost"),
        port=getEnv(port_key, 5432),
        password=getEnv(pw_key),
        options=getEnv(options_key, ""),
        database=getEnv(db_key, "postgres"),
        user=getEnv(user_key, "postgres"),
        cursor_factory=cursor_type,
    )
