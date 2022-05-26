from typing import Callable

import psycopg2.extras

from psycopg2.extensions import register_adapter, adapt

from .inputs.types import HTTPVerb, RequestBodySchema, KeywordType

http_verb_to_string : Callable[[HTTPVerb], str] = lambda v : v.name.lower()

verb_to_sql = lambda v : psycopg2.extensions.AsIs("".join(["'", http_verb_to_string(v), "'"]))
register_adapter(HTTPVerb, verb_to_sql)

register_adapter(RequestBodySchema, lambda s : psycopg2.extras.Json(s.schema))

register_adapter(set, lambda s : adapt(list(s)))

register_adapter(dict, psycopg2.extras.Json)

register_adapter(KeywordType, lambda v : psycopg2.extensions.AsIs("".join(["'", v.name, "'"])))

