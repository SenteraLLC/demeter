from typing import Any, Tuple, TypeVar, Set, Callable, Dict, List, Sequence, Literal
from typing import cast

import curses

import logging
logger = logging.getLogger()

from collections import OrderedDict

import psycopg2.extras

from dotenv import load_dotenv

from demeter.db import getConnection

#from .types import interactive_select
from .field_groups import interactive_select

if __name__ == '__main__':
  load_dotenv()

  #connection = getConnection(psycopg2.extras.RealDictCursor)
  connection = getConnection()
  cursor = connection.cursor()
  types = interactive_select(cursor)
  for t in types:
    print("T: ",t)


