import os
from ast import literal_eval
from typing import (
    Any,
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
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
) -> Connection:
    return getEngine(
        env_name=env_name, cursor_type=cursor_type, dialect=dialect
    ).connect()


def getEngine(
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
) -> Connection:
    db_meta = literal_eval(getEnv(env_name))  # make into dictionary
    schema_name = (
        db_meta["schema_name"] + "," if "schema_name" in db_meta.keys() else ""
    )
    connect_args = {
        "options": "-csearch_path={}public".format(schema_name),
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


def getSession(
    env_name: str = "TEST",
    cursor_type: Type[psycopg2.extensions.cursor] = psycopg2.extras.NamedTupleCursor,
    dialect: str = "postgresql+psycopg2",
):
    return sessionmaker(
        getEngine(env_name=env_name, cursor_type=cursor_type, dialect=dialect).connect()
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
