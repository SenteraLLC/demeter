from typing import TypeVar, Generic, Sequence, Callable, Set, Dict, Any, Optional, List
from typing import cast

import math
import curses
import curses.ascii
from collections import OrderedDict

from ..formatting import Margins, ColumnFormat
from ..formatting import inferColumnFormat, getSpecifiers

from ..theme import ColorScheme
from ..theme import setup_theme

from ..logging import getLogger
from ..summary import RawRowType
logger = getLogger()

def to_ascii(s : str) -> int:
  return ord(curses.ascii.ascii(s))

from .table import Table
from .header import Header
from .footer import Footer

class Picker(Generic[RawRowType]):
  def __init__(self,
               title : str,
               raw_rows : Sequence[RawRowType],
               margins : Margins,
               layout : OrderedDict[str, ColumnFormat],
               separator : str = "   ",
              ) -> None:
    setup_theme()

    self.layout = layout
    titles : List[str] = []
    for t, f in self.layout.items():
      titles.append(t)
      inferColumnFormat(f, t, raw_rows)

    self.specifiers = getSpecifiers(layout, separator, curses.COLS)

    self.title = title
    self.raw_rows = raw_rows
    self.margins = margins
    self.selected_rows : Set[int] = set()

    self.y_max = curses.LINES
    self.x_max = curses.COLS
    self.number_of_rows = len(self.raw_rows)

    self.table = Table(self.raw_rows, self.specifiers, self.margins, self.y_max, self.x_max, separator)

    w = self.table.get_width()
    m = self.margins

    h = self.table.format_row({ layout[t].key : t for t in layout }, w)
    #h = self.table.format_header()
    self.header = Header(title, h, w, m)

    rows = self.table.get_rows()
    coords = self.table.get_coords()
    self.footer = Footer(rows, w, m, coords.bottom)

    self.refresh()


  def refresh(self) -> None:
    po = self.table.get_page_offset()
    n = self.table.get_number_of_rows()
    v = self.table.get_number_of_visible_rows()
    self.header.refresh(po, n, v)

    co = self.table.get_cursor_offset()
    self.footer.refresh(co)

    self.table.refresh(self.selected_rows)


  # J - Down
  # K - Up
  # Enter - Done
  # Esc - Cancel
  # Space - Select
  def step(self) -> bool:
    cmd = self.table.get_curses_command()
    logger.warning("CMD: %s",str(cmd))

    ESC = 27
    if cmd == ESC:
      logger.warning("ESC.")
      return self._do_cancel()

    keep_going = True

    dy = {to_ascii('k') : -1,
          curses.KEY_UP : -1,
          to_ascii('j') : 1,
          curses.KEY_DOWN : 1,
         }.get(cmd, 0)
    if dy != 0:
      logger.warning("DY: %s",str(dy))
      keep_going = self._do_navigate(dy)
    elif cmd == to_ascii(' '):
      keep_going = self._do_select()
    elif cmd in [to_ascii('\n'), curses.KEY_ENTER]:
      logger.warning("ENTER.")
      keep_going = self._do_stop()

    self.refresh()

    return keep_going


  def _do_stop(self) -> bool:
    return True


  def _do_select(self) -> bool:
    co = self.table.get_cursor_offset()
    color_scheme = ColorScheme.DEFAULT
    try:
      self.selected_rows.remove(co)
      self.table.refresh_row(co, self.selected_rows, ColorScheme.CURSOR)
    except KeyError:
      self.selected_rows.add(co)
      self.table.refresh_cursor(co, self.selected_rows)

    return True


  def _do_navigate(self, dy : int) -> bool:
    if (dy != 0):
      return self.table.update_cursor(dy, self.selected_rows)
    return False


  def _do_end(self) -> bool:
    return False


  def _do_cancel(self) -> bool:
    return False


  def get_selected_rows(self) -> Sequence[RawRowType]:
    return [ self.raw_rows[i] for i in self.selected_rows ]


