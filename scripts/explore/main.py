from typing import Any, Sequence, Dict, Set
from collections import OrderedDict

from demeter.db import TableId

from .summary import Summary
from .data_option import DataOption
from .curses import SelectedResult

from . import types
from . import field_groups

from .values import getValues
from .fields import getFieldIds

from .logging import getLogger

logger = getLogger()

def main(cursor : Any,
         targets : Sequence[DataOption],
         output_directory : str,
        ) -> None:
  results : OrderedDict[DataOption, SelectedResult[Any]] = OrderedDict()

  for t in targets:
    logger.warn("TARGET: %s", t)
    if t == DataOption.LOCAL_TYPE:
      maybe_types = types.interactive_select(cursor, results)
      logger.warn("TYPES: %s", maybe_types)
      if (ts := maybe_types):
        results[t] = ts
    elif t == DataOption.FIELD_GROUP:
      maybe_groups = field_groups.interactive_select(cursor, results)
      if (gs := maybe_groups):
        results[t] = gs
    else:
      raise Exception("Option not supported: {option}")


  for n, selected_results in results.items():
    print("\nN: ",n)
    s = selected_results.selected
    for x in s:
      print(x)

  local_type_ids : Set[TableId] = set()
  if (lt := results.get(DataOption.LOCAL_TYPE)) is not None:
    local_type_ids = {s.local_type_id for s in lt.selected}

  #field_group_ids : Set[TableId] = set(results.get(DataOption.FIELD_GROUP, []))
  field_ids = getFieldIds(results)

  getValues(cursor, field_ids, local_type_ids)
