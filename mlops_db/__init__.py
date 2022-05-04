from typing import Callable

import psycopg2
import psycopg2.extras

from .lib.inputs.types import HTTPVerb, RequestBodySchema

http_verb_to_string : Callable[[HTTPVerb], str] = lambda v : v.name.lower()

psycopg2.extensions.register_adapter(HTTPVerb, lambda v : psycopg2.extensions.AsIs("".join(["'", http_verb_to_string(v), "'"])))
psycopg2.extensions.register_adapter(RequestBodySchema, lambda d : psycopg2.extras.Json(d.schema))

from psycopg2.extensions import register_adapter, adapt

register_adapter(set, lambda s : adapt(list(s)))
