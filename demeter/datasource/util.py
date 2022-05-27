from typing import Dict, Optional, List, Any, Type, Union

from ..types.execution import KeywordArgument


def createKeywordArguments(keyword_arguments : Dict[str, Any],
                           keyword_types     : Dict[str, Type],
                           execution_id      : int,
                           function_id       : int,
                          ) -> List[KeywordArgument]:
  out : List[KeywordArgument] = []
  for name, value in keyword_arguments.items():
    value_string : Optional[str] = None
    value_number : Optional[float] = None

    def set_string( v : str) -> None:
      nonlocal value_string
      value_string = v

    def set_number( v : Union[float, int]) -> None:
      nonlocal value_number
      value_number = float(v)

    typ = keyword_types[name]
    {str   : set_string,
     int   : set_number,
     float : set_number,
    }.get(typ, set_string)(value) # type: ignore
    ka = KeywordArgument(
           execution_id = execution_id,
           function_id = function_id,
           keyword_name = name,
           value_number = value_number,
           value_string = value_string,
         )
    out.append(ka)
  return out



