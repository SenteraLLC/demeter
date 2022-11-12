from typing import Callable

from collections import OrderedDict

import psycopg2
from psycopg2.extensions import register_adapter

from . import Table

from types import MappingProxyType


def register_sql_adapters() -> None:
    register_adapter(Table, lambda t: t())
    # register_adapter(OrderedDict, psycopg2.extras.Json)
    register_adapter(dict, psycopg2.extras.Json)
