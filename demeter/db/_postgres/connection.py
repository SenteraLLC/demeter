from typing import Optional, Any

import os

def getEnv(name : str, default : Optional[Any] = None, is_required : bool = False) -> Optional[str]:
  v = os.environ.get(name, default)
  maybe_x = v
  if is_required and v is None:
    raise Exception(f"Environment variable for '{name}' not set")
  return v


from psycopg2.extensions import connection as PGConnection
from psycopg2.extensions import register_adapter, adapt

import psycopg2
import psycopg2.extras

def getConnection() -> PGConnection:
  # TODO: Move this closer to wherever it is used
  register_adapter(set, lambda s : adapt(list(s))) # type: ignore

  host = getEnv("DEMETER_PG_HOST", "localhost")
  port = getEnv("DEMETER_PG_PORT", 5432)
  password = getEnv("DEMETER_PG_PASSWORD")
  user = getEnv("DEMETER_PG_USER", "postgres")
  options = getEnv("DEMETER_PG_OPTIONS", "")
  database = getEnv("DEMETER_PG_DATABASE", "postgres")
  return psycopg2.connect(host=host, port=port, password=password, options=options, database=database, user=user, cursor_factory=psycopg2.extras.NamedTupleCursor)


