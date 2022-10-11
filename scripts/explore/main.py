from typing import Any, Sequence
from collections import OrderedDict

from .summary import Summary
from .data_option import DataOption

from . import types
from . import field_groups


def main(cursor : Any,
         target : DataOption,
         filters : Sequence[DataOption],
         output_directory : str,
        ) -> None:
  results : OrderedDict[DataOption, Sequence[Summary]] = OrderedDict()

  to_get = list(filters) + [target]
  for option in to_get:
    if option == DataOption.LOCAL_TYPE:
      maybe_types = types.interactive_select(cursor)
      if (t := maybe_types):
        results[option] = t
    elif option == DataOption.FIELD_GROUP:
      maybe_groups = field_groups.interactive_select(cursor)
      if (g := maybe_groups):
        results[option] = g
    else:
      raise Exception("Option not supported: {option}")


  for n, rs in results.items():
    print("\nN: ",n)
    for x in rs:
      print("   X: ",x)

