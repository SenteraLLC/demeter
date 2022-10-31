from typing import Any, Tuple

from ... import db
from .types import HTTPType, HTTPVerb


def stringToHTTPVerb(s: str) -> HTTPVerb:
    return HTTPVerb[s.upper()]


def getHTTPTypeByName(cursor: Any, http_type_name: str) -> Tuple[db.TableId, HTTPType]:
    stmt = """select * from http_type where type_name = %(name)s"""
    cursor.execute(stmt, {"name": http_type_name})

    results = cursor.fetchall()
    if len(results) != 1:
        raise Exception(f"HTTP Type does exist for {http_type_name}")
    result_with_id = results[0]

    http_type_id = result_with_id.http_type_id
    result = {
        k: (stringToHTTPVerb(v) if k == "verb" else v)
        for k, v in result_with_id._asdict().items()
        if k != "http_type_id"
    }
    http_type = HTTPType(**result)

    return http_type_id, http_type
