from typing import Set, Dict, List, Sequence
from typing import cast

from demeter.db import TableId
from collections import OrderedDict

from .curses import FilterBy
from .data_option import DataOption
from .field_groups import FieldGroupSummary

from .log import getLogger
logger = getLogger()

def getFieldGroupFieldIds(target : FieldGroupSummary,
                          field_group_summaries: Dict[TableId, FieldGroupSummary],
                         ) -> Sequence[TableId]:
  logger.warn("TARGET: %s",target)
  out : List[TableId] = []
  out.extend([f["field_id"] for f in target.fields])
  for i in target.field_group_ids:
    g = field_group_summaries[i]
    out.extend(getFieldGroupFieldIds(g, field_group_summaries))
  logger.warn("LEN: %s",len(out))
  return out


def getFieldIds(filter_by : FilterBy) -> Set[TableId]:
  all_field_ids : Set[TableId] = set()
  if (field_group_result := filter_by[DataOption.FIELD_GROUP]):
    selected = field_group_result.selected
    id_to_field_group = cast(OrderedDict[TableId, FieldGroupSummary], field_group_result.results)
    for s in selected:
      s = cast(FieldGroupSummary, s)
      field_ids = getFieldGroupFieldIds(s, id_to_field_group)
      all_field_ids.update(field_ids)
  return all_field_ids
