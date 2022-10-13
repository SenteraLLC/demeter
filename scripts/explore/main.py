from typing import Any, Sequence
from collections import OrderedDict

from .summary import Summary
from .data_option import DataOption

from . import types
from . import field_groups


def main(cursor : Any,
         targets : Sequence[DataOption],
         output_directory : str,
        ) -> None:
  results : OrderedDict[DataOption, Sequence[Summary]] = OrderedDict()

  for t in targets:
    if t == DataOption.LOCAL_TYPE:
      maybe_types = types.interactive_select(cursor)
      if (ts := maybe_types):
        results[t] = ts
    elif t == DataOption.FIELD_GROUP:
      maybe_groups = field_groups.interactive_select(cursor)
      print("GROUPS: ",maybe_groups)
      if (gs := maybe_groups):
        results[t] = gs
    else:
      raise Exception("Option not supported: {option}")


  for n, rs in results.items():
    print("\nN: ",n)
    for x in rs:
      print("   X: ",x)

