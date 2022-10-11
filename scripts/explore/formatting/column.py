from typing import Set, Sequence, Any

from dataclasses import dataclass

from .options import FormatOptions

from ..logging import getLogger
logger = getLogger()


@dataclass(frozen=True)
class ColumnFormat:
  key : str
  options : FormatOptions = FormatOptions()
  minimum_fraction : float = 0.1

  def getWidth(self, title : str) -> int:
    return self.options.fixed_width or max(self.options.max_columns, len(title) + 1)

  def getAlignment(self) -> int:
    return -1 if self.options.align <= 0 else 1

  # TODO: Enable after testing
  #@cache
  def getSpecifier(self, title : str, align : int) -> str:
    w = self.getWidth(title)
    op = "<" if align < 0 else ">"
    return f"{op}{w}.{w}"

  def isRequired(self) -> bool:
    return self.options.is_required

  def shouldSkip(self, existing_titles : Set[str]) -> bool:
    is_excluded = self.options.xor_title in existing_titles
    if is_excluded:
      return True
    return self.options.do_skip



def _calculateFixedWidth(column_format : ColumnFormat,
                         title_width : int,
                        ) -> int:
  cf = column_format
  l = cf.options.min_columns
  h = cf.options.max_columns
  greatest_value_width = max(len(str(r[self.key] or '')) for r in rows) # type: ignore
  return max(title_width, min(h, max(greatest_value_width, l)))


# TODO: Use cache after debugging
#@cache
def inferColumnFormat(column_format : ColumnFormat,
                      title : str,
                      rows : Sequence[Any],
                     ) -> bool:
  cf = column_format
  if cf is None:
    k = column_format.key
    null_count = sum(1 for r in rows if r[k] is None)
    n = len(rows)
    not_null_fraction = 1 - (null_count / n)
    if not_null_fraction < cf.minimum_fraction:
      cf.options.do_skip = True
      logger.warning(f"Fraction too low ({not_null_fraction}) for {title}")
      return True

    title_width = len(title)
    cf.options.fixed_width = _calculateFixedWidth(cf, title_width)

    return True

  return False


