from typing import Callable

import psycopg2
from psycopg2.extensions import register_adapter

from . import HTTPVerb, KeywordType


def register_sql_adapters() -> None:
    http_verb_to_string: Callable[[HTTPVerb], str] = lambda v: v.name.lower()
    verb_to_sql = lambda v: psycopg2.extensions.AsIs(
        "".join(["'", http_verb_to_string(v), "'"])
    )
    register_adapter(HTTPVerb, verb_to_sql)
    register_adapter(
        KeywordType, lambda v: psycopg2.extensions.AsIs("".join(["'", v.name, "'"]))
    )
