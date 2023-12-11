from dataclasses import dataclass
from typing import Optional

from demeter import db


@dataclass(frozen=True)
class Grouper(db.Detailed):
    """Arbitrary collection of Field, FieldTrial, Plot, or other Grouper objects which allows demeter to represent any
    grouping of objects, which allows for a flexible organization scheme across customers.
    """

    name: str
    organization_id: db.TableId
    parent_grouper_id: Optional[db.TableId] = None
