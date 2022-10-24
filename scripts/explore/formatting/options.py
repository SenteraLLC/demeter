from dataclasses import dataclass

from typing import Optional


@dataclass
class Margins:
  top : int
  bottom : int
  right : int
  left : int


@dataclass
class FormatOptions:
  is_required : bool = True
  align       : int  = -1

  fixed_width : Optional[int] = None

  # If fixed_width is undefined, use min/max
  min_columns : int = 2
  max_columns : int = 40

  xor_title : Optional[str] = None
  do_skip : bool = False

  def __post_init__(self) -> None:
    if self.min_columns <= 0:
      raise Exception("Minimum columns cannot be set <= 0. Please use 'is_required'.")
    if self.min_columns >= self.max_columns:
      raise Exception("Minimum columns cannot be less than maximum columns.")

