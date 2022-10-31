from collections import OrderedDict
from types import MappingProxyType
from typing import Callable

import psycopg2
from psycopg2.extensions import register_adapter

from . import Table


def register_sql_adapters() -> None:
    register_adapter(Table, lambda t: t())
    # register_adapter(OrderedDict, psycopg2.extras.Json)
    register_adapter(dict, psycopg2.extras.Json)
