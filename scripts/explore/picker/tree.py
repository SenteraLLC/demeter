from typing import Generic, Dict, Sequence, List, Optional, Tuple, Set, Generic

from collections import OrderedDict
import curses
from dataclasses import dataclass

from demeter.db import TableId

from .picker import Picker, Command
from .picker import to_ascii

from .table import Table

from ..theme import ColorScheme
from ..summary import RawRowType
from ..formatting import Margins, ColumnFormat
from ..formatting import inferColumnFormat

from ..log import getLogger
logger = getLogger()


@dataclass
class Cached(Generic[RawRowType]):
  table : Table[RawRowType]
  selected : Set[int]

Cache = Dict[TableId, Cached[RawRowType]]


class PickerTree(Generic[RawRowType]):
  def __init__(self,
               title   : str,
               margins : Margins,
               layout : OrderedDict[str, ColumnFormat],
               id_to_row : Dict[TableId, RawRowType],
               separator : str = "   ",
              ) -> None:

    self.layout = layout

    self.id_to_row = id_to_row
    rows = list(id_to_row.values())

    for t, f in self.layout.items():
      inferColumnFormat(f, t, rows)

    roots = [ r for r in rows if r.depth == 0 ] # type: ignore

    self.title = title
    t = self.title
    self.picker = Picker(t,
                         roots,
                         margins,
                         layout,
                         separator
                        )

    none_selected : Set[int] = set()

    self.root_id = TableId(-987654321987654321)
    self.current_id = self.root_id
    self.id_to_cached : Cache[RawRowType] = {
      self.root_id : Cached(self.picker.table, none_selected)
    }

    self.stack : List[TableId] = []


  def _clear(self, table : Table[RawRowType]) -> None:
    t = table
    blank_row = " " * t.width
    w = t.width
    h = t.height
    to_clear = curses.newwin(h + 1, w, t.top, t.left)
    for i in range(h):
      to_clear.addnstr(i, 0, blank_row, w, curses.color_pair(ColorScheme.DEFAULT))
    to_clear.refresh()


  def _get_rows(self, row_ids : Sequence[TableId]) -> List[RawRowType]:
    return [ self.id_to_row[i] for i in row_ids ]


  def _get_table_and_selected(self,
                              table_id : TableId,
                              row_ids : Sequence[TableId],
                             ) -> Tuple[Table[RawRowType], Set[int]]:
    try:
      cached = self.id_to_cached[table_id]
      return (cached.table, cached.selected)
    except KeyError:
      p = self.picker
      rows = self._get_rows(row_ids)
      table = Table(rows, p.specifiers, p.margins, p.y_max, p.x_max, p.separator)
      none_selected : Set[int] = set()

      self.id_to_cached[table_id] = Cached(table, none_selected)
      return self._get_table_and_selected(table_id, row_ids)

  def _swap_table(self, table : Table[RawRowType], selected : Set[int]) -> bool:
    self._clear(table)

    p = self.picker
    p.table = table
    p.selected_rows = selected
    p.table.draw(selected)
    p.table.refresh(selected)

    return True


  def _back(self) -> bool:
    table_id = self.stack.pop()
    c = cached = self.id_to_cached[table_id]
    self._swap_table(c.table, c.selected)
    self.current_id = table_id
    return True


  def step(self) -> bool:
    cmd = self.picker.get_command()
    keep_going = self.picker.step(cmd)

    maybe_new_table : Optional[Table[RawRowType]] = None
    p = self.picker

    if cmd == Command.ENTER:
      if not p.is_selected():
        r = p.get_cursor_row()
        child_ids = r.field_group_ids # type: ignore
        if child_ids:

          _id = r.field_group_id # type: ignore

          child_table, child_selected = self._get_table_and_selected(_id, child_ids)

          keep_going = self._swap_table(child_table, child_selected)

          self.stack.append(self.current_id)
          self.current_id = _id

    elif cmd == Command.BACK:
      try:
        self._back()
      except IndexError:
        pass

    return keep_going


  def _get_selected_rows(self,
                         table : Table[RawRowType],
                         selected : Set[int],
                        ) -> Sequence[RawRowType]:
    selected_rows = [ table.raw_rows[i] for i in selected ]
    selected_ids = { r.field_group_id for r in selected_rows } # type: ignore
    child_ids = { r.field_group_id for r in table.raw_rows } # type: ignore
    for _id in child_ids:
      r = self.id_to_row[_id]

      if _id not in selected_ids:
        try:
          c = self.id_to_cached[_id]
          more_selected_rows = self._get_selected_rows(c.table, c.selected)
          selected_rows.extend(more_selected_rows)
        except KeyError:
          pass

    return selected_rows


  def get_selected_rows(self) -> Sequence[RawRowType]:
    c = self.id_to_cached[self.root_id]
    return self._get_selected_rows(c.table, c.selected)


