from typing import Optional

import os

def getEnv(name : str, default : Optional[str] = None) -> str:
  v = os.environ.get(name, default)
  if v is None:
    raise Exception(f"Environment variable for '{name}' not set")
  return v


from psycopg2.extensions import connection as PGConnection
from psycopg2.extensions import register_adapter, adapt

import psycopg2
import psycopg2.extras

import getpass

def getConnection() -> PGConnection:
  # TODO: Move this closer to wherever it is used
  register_adapter(set, lambda s : adapt(list(s))) # type: ignore

  host = getEnv("DEMETER_PG_HOST", "localhost")
  user = getEnv("DEMETER_PG_USER", getpass.getuser())
  options = getEnv("DEMETER_PG_OPTIONS", "")
  database = getEnv("DEMETER_PG_DATABASE", "postgres")
  return psycopg2.connect(host=host, options=options, database=database, user=user, cursor_factory=psycopg2.extras.NamedTupleCursor)


