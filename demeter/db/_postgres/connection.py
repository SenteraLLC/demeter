import os
from ast import literal_eval

from typing import Any, Optional

from typing import Type

import psycopg2
import psycopg2.extras
from psycopg2.extensions import adapt
from psycopg2.extensions import connection
from psycopg2.extensions import register_adapter
from sqlalchemy.engine import Connection, create_engine, URL


def getEnv(
    name: str,
    default: Optional[Any] = None,
    is_required: bool = False,
) -> Optional[str]:

    v = os.environ.get(name, default)

    if is_required and v is None:
        raise Exception(f"Environment variable for '{name}' not set")
    return v


def getConnection(
    env_name: str,
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
) -> Connection:

    db_meta = literal_eval(getEnv(env_name))  # make into dictionary

    connect_args = {
        "options": "-csearch_path={},public".format(db_meta["schema_name"]),
        "cursor_factory": cursor_type,
    }

    url_object = URL.create(
        dialect,
        host=db_meta["host"],
        port=db_meta["port"],
        username=db_meta["user"],
        password=db_meta["password"],
        database=db_meta["database"],
    )
    engine = create_engine(url_object, connect_args=connect_args)
    return engine.connect()


def getConnection_psycopg2(
    env_name: str,
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
) -> connection:
    register_adapter(set, lambda s: adapt(list(s)))  # type: ignore - not sure what this does?

    db_meta = literal_eval(getEnv(env_name))  # make into dictionary
    options = "-c search_path={},public".format(db_meta["schema_name"])

    return psycopg2.connect(
        host=db_meta["host"],
        port=db_meta["port"],
        password=db_meta["password"],
        options=options,
        database=db_meta["database"],
        user=db_meta["user"],
        cursor_factory=cursor_type,
    )
