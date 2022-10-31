from typing import Sequence, Union, cast

from psycopg2.sql import SQL, Composable, Composed


# NOTE: Psycopg2.sql method stubs are untyped
#   These 'overrides' will have to do for now
def doPgJoin(sep: str, args: Sequence[Composable]) -> Composed:
    return cast(Composed, SQL(sep).join(args))  # type: ignore


def doPgFormat(template: str, *args: Composable, **kwargs: Composable) -> Composed:
    return cast(Composed, SQL(template).format(*args, **kwargs))  # type: ignore
