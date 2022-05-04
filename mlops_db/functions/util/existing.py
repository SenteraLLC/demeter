from typing import Optional, Sequence, Any

from ...lib.execution.types import ExecutionSummary

# TODO: Add more ways to tune "matching"
#         There could be some keyword meta-arguments for tuning
#         the duplicate detection heuristics
def getExistingDuplicate(existing_executions : Sequence[ExecutionSummary],
                         execution_summary : ExecutionSummary,
                        ) -> Optional[ExecutionSummary]:
  # Allow matching on minor versions of a function
  blacklist = ["function_id", "execution_id"]
  def eq(left : Any,
         right : Any,
        ) -> bool:
    for left_key, left_value in left.items():
      if left_key in blacklist:
        continue
      try:
        right_value = right[left_key]
      except KeyError:
        right_value = None
      if all(type(v) == dict for v in [left_value, right_value]):
        nested_eq = eq(left_value, right_value)
        if not nested_eq:
          return nested_eq
      elif all(type(v) == list for v in [left_value, right_value]):
        if len(left_value) != len(right_value):
          return False
        sort_by = lambda d : sorted(d.items())
        left_sorted = sorted(left_value, key=sort_by)
        right_sorted = sorted(right_value, key=sort_by)
        for l, r in zip(left_sorted, right_sorted):
          nested_eq = eq(l, r)
          if not nested_eq:
            return nested_eq
      elif left_value != right_value:
        return False
    return True

  for e in existing_executions:
    inputs = e["inputs"]
    if eq(inputs, execution_summary["inputs"]):
      return e
  return None


