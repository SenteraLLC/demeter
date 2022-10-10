from typing import Sequence

from .table import RawRowType, Row

import curses

from ..formatting import Margins
from ..theme import ColorScheme

from dataclasses import asdict

import logging
logger = logging.getLogger()

class Footer:
  def __init__(self,
               rows : Sequence[Row[RawRowType]],
               width : int,
               margins : Margins,
               table_bottom : int,
              ) -> None:
    self.rows = rows
    self.width = width
    self.margins = margins
    self.footer = curses.newwin(self.margins.bottom + 1, width, table_bottom + 1, margins.left)


  def refresh(self,
              cursor_offset : int,
             ) -> None:
    if (height := self.margins.bottom) > 0:
      raw = self.rows[cursor_offset].raw

      # TODO: Improve prettification
      # NOTE: 's' is consumed by the for-loop
      s = str(asdict(raw))

      w = self.width
      for i in range(height):
        next_line, s = s[:w], s[w:]
        next_line = f"{next_line:<{w}.{w}}"
        self.footer.addnstr(i, 1, next_line, w - 1, curses.color_pair(ColorScheme.MENU_TEXT))

    self.footer.refresh()

