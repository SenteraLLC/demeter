from typing import TypedDict, Optional, Dict, Any

from datetime import datetime


class Table(TypedDict):
  pass

class Updateable(Table):
  last_updated : Optional[datetime]

Details = Optional[Dict[str, Any]]

class Detailed(Updateable):
  details : Details

class TypeTable(Table):
  pass

class TableKey(TypedDict):
  pass

